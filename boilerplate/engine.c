/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    int stop_requested;
    int stdout_fd;
    int stderr_fd;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
    int stdout_fd;
    int stderr_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i++) {
        char *end = NULL;
        long nice_value;

        /* Skip arguments that don't look like flags */
        if (argv[i][0] != '-')
            continue;

        /* Do we have a value for this flag? */
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            i++;  /* skip the value we just consumed */
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            i++;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            i++;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

/* bounded_buffer_begin_shutdown removed - functionality duplicated in supervisor cleanup */

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    
    /* Wait if buffer is full */
    while (buffer->count >= LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }
    
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    
    /* Insert item */
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    
    /* Wake consumer */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    
    /* Wait if buffer is empty */
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }
    
    /* If shutdown and empty, return -1 */
    if (buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    
    /* Extract item */
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    
    /* Wake producer */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    
    while (1) {
        /* Pop items from buffer and write to log files */
        if (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
            /* Got an item, write to log file using full path */
            char logpath[PATH_MAX];
            FILE *logfile;
            time_t now;
            struct tm *timeinfo;
            char timestamp[32];
            
            snprintf(logpath, PATH_MAX, "logs/%s.log", item.container_id);
            logfile = fopen(logpath, "a");
            
            if (logfile != NULL) {
                /* Write timestamp and container ID to show producer/consumer pipeline activity */
                now = time(NULL);
                timeinfo = localtime(&now);
                strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", timeinfo);
                fprintf(logfile, "[%s] [%s] ", timestamp, item.container_id);
                
                /* Write the actual container output */
                fwrite(item.data, 1, item.length, logfile);
                fflush(logfile);
                fclose(logfile);
            }
        } else {
            /* Buffer is empty and shutting down */
            break;
        }
    }
    
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    
    /* Redirect stdout and stderr to pipes */
    if (cfg->stdout_fd >= 0) {
        dup2(cfg->stdout_fd, STDOUT_FILENO);
        close(cfg->stdout_fd);
    }
    if (cfg->stderr_fd >= 0) {
        dup2(cfg->stderr_fd, STDERR_FILENO);
        close(cfg->stderr_fd);
    }
    
    /* chroot into container rootfs FIRST (before any mounts) */
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }
    
    if (chdir("/") < 0) {
        perror("chdir");
        return 1;
    }
    
    /* Now mount /proc inside the container at /proc */
    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount /proc");
    }
    
    /* Set nice value if requested */
    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) < 0) {
            perror("nice");
            return 1;
        }
    }
    
    /* Parse command and exec */
    if (cfg->command[0] == '\0') {
        fprintf(stderr, "Error: empty command\n");
        return 1;
    }
    
    char *cmd_copy = strdup(cfg->command);
    if (cmd_copy == NULL) {
        perror("strdup");
        return 1;
    }
    
    char *args[128] = {NULL};
    int argc = 0;
    char *token = strtok(cmd_copy, " ");
    
    while (token != NULL && argc < 127) {
        args[argc++] = token;
        token = strtok(NULL, " ");
    }
    args[argc] = NULL;
    
    if (argc > 0) {
        execvp(args[0], args);
        perror("execvp");
    }
    
    free(cmd_copy);
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}


static supervisor_ctx_t *g_ctx = NULL;

static void sigchld_handler(int sig)
{
    (void)sig;
    /* Mark that we need to reap */
}

static void sigint_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    int rc;
    char buf[512];
    pid_t pid;
    int status;
    
    (void)rootfs;
    
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    ctx.containers = NULL;
    g_ctx = &ctx;
    
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }
    
    /* Open kernel monitor device */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        ctx.monitor_fd = -1;
    
    /* Prevent socket() from using FD 0-2 (standard streams) */
    /* Open /dev/null and use it to fill FDs 0-2 */
    int null_fd = open("/dev/null", O_RDWR);
    if (null_fd >= 0) {
        if (null_fd == 0) {
            /* Got FD 0, now dup to get 1 and 2 */
            dup2(null_fd, 1);
            dup2(null_fd, 2);
        } else if (null_fd == 1) {
            /* Got FD 1, ensure 0 is filled */
            dup2(null_fd, 0);
            dup2(null_fd, 2);
            close(null_fd);
        } else if (null_fd == 2) {
            /* Got FD 2, ensure 0 and 1 are filled */
            dup2(null_fd, 0);
            dup2(null_fd, 1);
            close(null_fd);
        } else {
            /* Got FD 3+, stdin/stdout/stderr already set */
            close(null_fd);
        }
    }
    
    /* Create Unix domain socket for control IPC */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(ctx.server_fd);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    
    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        close(ctx.server_fd);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    
    /* Create logs directory for container output */
    mkdir("logs", 0755);
    
    /* Initialize bounded buffer */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        close(ctx.server_fd);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    
    /* Start logging thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        bounded_buffer_destroy(&ctx.log_buffer);
        close(ctx.server_fd);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    
    /* Set up signal handlers */
    signal(SIGCHLD, sigchld_handler);
    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);
    
    printf("Supervisor started, socket at %s\n", CONTROL_PATH);
    
    /* Main supervisor loop */
    while (!ctx.should_stop) {
        fd_set readfds;
        struct timeval tv;
        control_request_t req;
        control_response_t resp;
        int client_fd;
        ssize_t n;
        int max_fd;
        
        /* Set up for select with timeout to handle SIGCHLD */
        FD_ZERO(&readfds);
        FD_SET(ctx.server_fd, &readfds);
        max_fd = ctx.server_fd;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        
        /* Add container pipes to select */
        pthread_mutex_lock(&ctx.metadata_lock);
        container_record_t *rec = ctx.containers;
        while (rec != NULL) {
            if (rec->stdout_fd >= 0) {
                FD_SET(rec->stdout_fd, &readfds);
                if (rec->stdout_fd > max_fd)
                    max_fd = rec->stdout_fd;
            }
            if (rec->stderr_fd >= 0) {
                FD_SET(rec->stderr_fd, &readfds);
                if (rec->stderr_fd > max_fd)
                    max_fd = rec->stderr_fd;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx.metadata_lock);
        
        /* Reap any exited children */
        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = ctx.containers;
            while (rec != NULL) {
                if (rec->host_pid == pid) {
                    rec->state = CONTAINER_EXITED;
                    if (WIFEXITED(status)) {
                        rec->exit_code = WEXITSTATUS(status);
                        rec->exit_signal = 0;
                    } else if (WIFSIGNALED(status)) {
                        rec->exit_code = -1;
                        rec->exit_signal = WTERMSIG(status);
                    }
                    /* Unregister from kernel monitor */
                    if (ctx.monitor_fd >= 0)
                        unregister_from_monitor(ctx.monitor_fd, rec->id, pid);
                    break;
                }
                rec = rec->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
        }
        
        /* Accept new connections with timeout */
        int sel = select(max_fd + 1, &readfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EBADF) {
                /* One of the FDs became invalid (likely closed after we built the set) */
                /* Just skip this iteration and try again */
                continue;
            }
            perror("select");
            break;
        }
        
        /* Producer side: read from container pipes (if any) */
        if (sel > 0) {
            pthread_mutex_lock(&ctx.metadata_lock);
            rec = ctx.containers;
            while (rec != NULL) {
                container_record_t *next = rec->next;
                char buf[LOG_CHUNK_SIZE];
                ssize_t bytes_read;
                
                /* Read from stdout */
                if (rec->stdout_fd >= 0 && FD_ISSET(rec->stdout_fd, &readfds)) {
                    bytes_read = read(rec->stdout_fd, buf, LOG_CHUNK_SIZE);
                    if (bytes_read > 0) {
                        log_item_t item;
                        memset(&item, 0, sizeof(item));
                        strncpy(item.container_id, rec->id, sizeof(item.container_id) - 1);
                        item.length = bytes_read;
                        memcpy(item.data, buf, bytes_read);
                        pthread_mutex_unlock(&ctx.metadata_lock);
                        bounded_buffer_push(&ctx.log_buffer, &item);
                        pthread_mutex_lock(&ctx.metadata_lock);
                    } else if (bytes_read == 0) {
                        /* EOF: close the pipe */
                        close(rec->stdout_fd);
                        rec->stdout_fd = -1;
                    } else {
                        /* Error: close the pipe */
                        close(rec->stdout_fd);
                        rec->stdout_fd = -1;
                    }
                }
                
                /* Read from stderr */
                if (rec->stderr_fd >= 0 && FD_ISSET(rec->stderr_fd, &readfds)) {
                    bytes_read = read(rec->stderr_fd, buf, LOG_CHUNK_SIZE);
                    if (bytes_read > 0) {
                        log_item_t item;
                        memset(&item, 0, sizeof(item));
                        strncpy(item.container_id, rec->id, sizeof(item.container_id) - 1);
                        item.length = bytes_read;
                        memcpy(item.data, buf, bytes_read);
                        pthread_mutex_unlock(&ctx.metadata_lock);
                        bounded_buffer_push(&ctx.log_buffer, &item);
                        pthread_mutex_lock(&ctx.metadata_lock);
                    } else if (bytes_read == 0) {
                        /* EOF: close the pipe */
                        close(rec->stderr_fd);
                        rec->stderr_fd = -1;
                    } else {
                        /* Error: close the pipe */
                        close(rec->stderr_fd);
                        rec->stderr_fd = -1;
                    }
                }
                
                rec = next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
        }
        
        if (sel == 0)
            continue;

        /* Only accept when the server socket is actually ready. */
        if (!FD_ISSET(ctx.server_fd, &readfds))
            continue;
        
        client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }
        
        /* Read control request */
        n = read(client_fd, &req, sizeof(req));
        if (n != (ssize_t)sizeof(req)) {
            close(client_fd);
            continue;
        }
        
        memset(&resp, 0, sizeof(resp));
        resp.status = 0;
        
        if (req.kind == CMD_START) {
            /* Create container */
            char stack[STACK_SIZE];
            child_config_t child_cfg;
            int pipefd_out[2], pipefd_err[2];
            
            /* Create pipes for stdout/stderr */
            if (pipe(pipefd_out) < 0 || pipe(pipefd_err) < 0) {
                perror("pipe");
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "pipe creation failed");
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }
            
            memset(&child_cfg, 0, sizeof(child_cfg));
            strncpy(child_cfg.id, req.container_id, sizeof(child_cfg.id) - 1);
            strncpy(child_cfg.rootfs, req.rootfs, sizeof(child_cfg.rootfs) - 1);
            strncpy(child_cfg.command, req.command, sizeof(child_cfg.command) - 1);
            child_cfg.nice_value = req.nice_value;
            child_cfg.log_write_fd = -1;
            child_cfg.stdout_fd = pipefd_out[1];
            child_cfg.stderr_fd = pipefd_err[1];
            
            /* Clone with namespaces */
            pid = clone(child_fn, stack + STACK_SIZE,
                       CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                       &child_cfg);
            
            /* Close write ends in parent */
            close(pipefd_out[1]);
            close(pipefd_err[1]);
            
            if (pid < 0) {
                perror("clone");
                close(pipefd_out[0]);
                close(pipefd_err[0]);
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "clone failed");
            } else {
                /* Track container */
                container_record_t *rec = malloc(sizeof(*rec));
                if (rec) {
                    memset(rec, 0, sizeof(*rec));
                    strncpy(rec->id, req.container_id, sizeof(rec->id) - 1);
                    rec->host_pid = pid;
                    rec->started_at = time(NULL);
                    rec->state = CONTAINER_RUNNING;
                    rec->soft_limit_bytes = req.soft_limit_bytes;
                    rec->hard_limit_bytes = req.hard_limit_bytes;
                    rec->exit_code = -1;
                    rec->stop_requested = 0;
                    rec->stdout_fd = pipefd_out[0];
                    rec->stderr_fd = pipefd_err[0];
                    
                    snprintf(rec->log_path, PATH_MAX, "logs/%s.log", req.container_id);
                    
                    pthread_mutex_lock(&ctx.metadata_lock);
                    rec->next = ctx.containers;
                    ctx.containers = rec;
                    pthread_mutex_unlock(&ctx.metadata_lock);
                    
                    /* Register with kernel monitor */
                    if (ctx.monitor_fd >= 0)
                        register_with_monitor(ctx.monitor_fd, req.container_id, pid,
                                            req.soft_limit_bytes, req.hard_limit_bytes);
                    
                    snprintf(resp.message, sizeof(resp.message),
                            "Container %s started with PID %d", req.container_id, pid);
                } else {
                    close(pipefd_out[0]);
                    close(pipefd_err[0]);
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "malloc failed");
                }
            }
        } else if (req.kind == CMD_RUN) {
            /* Create container and wait for it */
            char stack[STACK_SIZE];
            child_config_t child_cfg;
            int child_status;
            int pipefd_out[2], pipefd_err[2];
            
            /* Create pipes for stdout/stderr */
            if (pipe(pipefd_out) < 0 || pipe(pipefd_err) < 0) {
                perror("pipe");
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "pipe creation failed");
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }
            
            memset(&child_cfg, 0, sizeof(child_cfg));
            strncpy(child_cfg.id, req.container_id, sizeof(child_cfg.id) - 1);
            strncpy(child_cfg.rootfs, req.rootfs, sizeof(child_cfg.rootfs) - 1);
            strncpy(child_cfg.command, req.command, sizeof(child_cfg.command) - 1);
            child_cfg.nice_value = req.nice_value;
            child_cfg.log_write_fd = -1;
            child_cfg.stdout_fd = pipefd_out[1];
            child_cfg.stderr_fd = pipefd_err[1];
            
            pid = clone(child_fn, stack + STACK_SIZE,
                       CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                       &child_cfg);
            
            /* Close write ends in parent */
            close(pipefd_out[1]);
            close(pipefd_err[1]);
            
            if (pid < 0) {
                perror("clone");
                close(pipefd_out[0]);
                close(pipefd_err[0]);
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "clone failed");
            } else {
                /* Track container */
                container_record_t *rec = malloc(sizeof(*rec));
                if (rec) {
                    memset(rec, 0, sizeof(*rec));
                    strncpy(rec->id, req.container_id, sizeof(rec->id) - 1);
                    rec->host_pid = pid;
                    rec->started_at = time(NULL);
                    rec->state = CONTAINER_RUNNING;
                    rec->soft_limit_bytes = req.soft_limit_bytes;
                    rec->hard_limit_bytes = req.hard_limit_bytes;
                    rec->exit_code = -1;
                    rec->stop_requested = 0;
                    rec->stdout_fd = pipefd_out[0];
                    rec->stderr_fd = pipefd_err[0];
                    snprintf(rec->log_path, PATH_MAX, "logs/%s.log", req.container_id);
                    
                    pthread_mutex_lock(&ctx.metadata_lock);
                    rec->next = ctx.containers;
                    ctx.containers = rec;
                    pthread_mutex_unlock(&ctx.metadata_lock);
                    
                    /* Register with kernel monitor */
                    if (ctx.monitor_fd >= 0)
                        register_with_monitor(ctx.monitor_fd, req.container_id, pid,
                                            req.soft_limit_bytes, req.hard_limit_bytes);
                    
                    /* Send response immediately - don't block on waitpid */
                    /* Main loop will handle reaping via SIGCHLD */
                    snprintf(resp.message, sizeof(resp.message),
                            "Container %s started with PID %d", req.container_id, pid);
                    resp.status = 0;
                } else {
                    close(pipefd_out[0]);
                    close(pipefd_err[0]);
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "malloc failed");
                }
            }
        } else if (req.kind == CMD_PS) {
            /* List containers */
            snprintf(buf, sizeof(buf), "%-20s %-10s %-10s %-20s %-10s\n",
                    "ID", "PID", "STATE", "STARTED", "EXIT");
            write(client_fd, buf, strlen(buf));
            
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = ctx.containers;
            while (rec != NULL) {
                snprintf(buf, sizeof(buf), "%-20s %-10d %-10s %-20ld %-10d\n",
                        rec->id, rec->host_pid, state_to_string(rec->state),
                        rec->started_at, rec->exit_code);
                write(client_fd, buf, strlen(buf));
                rec = rec->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
        } else if (req.kind == CMD_LOGS) {
            /* Read and send log file */
            FILE *logfile = fopen(req.container_id, "r");
            if (logfile == NULL) {
                /* Build log path */
                snprintf(buf, sizeof(buf), "logs/%s.log", req.container_id);
                logfile = fopen(buf, "r");
            }
            
            if (logfile != NULL) {
                while (fgets(buf, sizeof(buf), logfile) != NULL) {
                    write(client_fd, buf, strlen(buf));
                }
                fclose(logfile);
                snprintf(resp.message, sizeof(resp.message), "Log sent");
            } else {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "Log file not found");
            }
        } else if (req.kind == CMD_STOP) {
            /* Find and stop container */
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = ctx.containers;
            int found = 0;
            while (rec != NULL) {
                if (strcmp(rec->id, req.container_id) == 0) {
                    found = 1;
                    if (rec->state == CONTAINER_RUNNING) {
                        rec->stop_requested = 1;
                        kill(rec->host_pid, SIGTERM);
                        snprintf(resp.message, sizeof(resp.message),
                                "Sent SIGTERM to container %s", req.container_id);
                    } else {
                        snprintf(resp.message, sizeof(resp.message),
                                "Container %s is not running", req.container_id);
                    }
                    break;
                }
                rec = rec->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            
            if (!found) {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                        "Container %s not found", req.container_id);
            }
        } else {
            resp.status = 1;
            snprintf(resp.message, sizeof(resp.message), "Unknown command");
        }
        
        write(client_fd, &resp, sizeof(resp));
        close(client_fd);
    }
    
    /* Cleanup */
    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    
    /* Close all remaining pipe file descriptors */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *rec = ctx.containers;
    while (rec != NULL) {
        if (rec->stdout_fd >= 0) close(rec->stdout_fd);
        if (rec->stderr_fd >= 0) close(rec->stderr_fd);
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    
    /* Signal logger thread to shutdown */
    pthread_mutex_lock(&ctx.log_buffer.mutex);
    ctx.log_buffer.shutting_down = 1;
    pthread_cond_broadcast(&ctx.log_buffer.not_empty);
    pthread_mutex_unlock(&ctx.log_buffer.mutex);
    
    /* Join logger thread */
    pthread_join(ctx.logger_thread, NULL);
    
    /* Destroy bounded buffer synchronization */
    pthread_mutex_destroy(&ctx.log_buffer.mutex);
    pthread_cond_destroy(&ctx.log_buffer.not_empty);
    pthread_cond_destroy(&ctx.log_buffer.not_full);
    
    /* Cleanup container records */
    pthread_mutex_lock(&ctx.metadata_lock);
    rec = ctx.containers;
    while (rec != NULL) {
        container_record_t *next = rec->next;
        free(rec);
        rec = next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    
    pthread_mutex_destroy(&ctx.metadata_lock);
    
    printf("Supervisor stopped\n");
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    struct sockaddr_un addr;
    int sockfd;
    control_response_t resp;
    ssize_t n;

    /* ps/logs stream text first, then trailer control_response_t. */
    if (req->kind == CMD_PS || req->kind == CMD_LOGS) {
        char chunk[512];
        char *all = NULL;
        size_t total = 0;
        size_t cap = 0;

        sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (sockfd < 0) {
            perror("socket");
            return 1;
        }

        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            perror("connect to supervisor");
            close(sockfd);
            return 1;
        }

        if (write(sockfd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
            perror("write request");
            close(sockfd);
            return 1;
        }

        while ((n = read(sockfd, chunk, sizeof(chunk))) > 0) {
            if (total + (size_t)n > cap) {
                size_t new_cap = (cap == 0) ? 1024 : cap;
                while (new_cap < total + (size_t)n)
                    new_cap *= 2;
                char *tmp = realloc(all, new_cap);
                if (tmp == NULL) {
                    perror("realloc");
                    free(all);
                    close(sockfd);
                    return 1;
                }
                all = tmp;
                cap = new_cap;
            }
            memcpy(all + total, chunk, (size_t)n);
            total += (size_t)n;
        }

        if (n < 0) {
            perror("read response");
            free(all);
            close(sockfd);
            return 1;
        }

        if (total < sizeof(resp)) {
            fprintf(stderr, "Invalid response from supervisor\n");
            free(all);
            close(sockfd);
            return 1;
        }

        size_t payload_len = total - sizeof(resp);
        memcpy(&resp, all + payload_len, sizeof(resp));

        if (payload_len > 0)
            fwrite(all, 1, payload_len, stdout);

        if (resp.message[0] != '\0')
            printf("%s\n", resp.message);

        free(all);
        close(sockfd);
        return resp.status;
    }
    
    sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }
    
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    
    if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect to supervisor");
        close(sockfd);
        return 1;
    }
    
    if (write(sockfd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(sockfd);
        return 1;
    }
    
    n = read(sockfd, &resp, sizeof(resp));
    if (n != (ssize_t)sizeof(resp)) {
        perror("read response");
        close(sockfd);
        return 1;
    }
    
    if (resp.message[0] != '\0')
        printf("%s\n", resp.message);
    
    close(sockfd);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    int flag_start = 5;
    int i;
    size_t cmd_len;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    /* Collect command and its arguments until we hit a flag (--soft-mib, etc.) */
    for (i = 5; i < argc && argv[i][0] != '-'; i++)
        flag_start = i + 1;

    /* Build full command string from argv[4] to first flag */
    cmd_len = 0;
    for (i = 4; i < flag_start && i < argc; i++) {
        if (i > 4)
            cmd_len += snprintf(req.command + cmd_len, sizeof(req.command) - cmd_len - 1, " ");
        cmd_len += snprintf(req.command + cmd_len, sizeof(req.command) - cmd_len - 1, "%s", argv[i]);
    }

    if (parse_optional_flags(&req, argc, argv, flag_start) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    int flag_start = 5;
    int i;
    size_t cmd_len;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    /* Collect command and its arguments until we hit a flag (--soft-mib, etc.) */
    for (i = 5; i < argc && argv[i][0] != '-'; i++)
        flag_start = i + 1;

    /* Build full command string from argv[4] to first flag */
    cmd_len = 0;
    for (i = 4; i < flag_start && i < argc; i++) {
        if (i > 4)
            cmd_len += snprintf(req.command + cmd_len, sizeof(req.command) - cmd_len - 1, " ");
        cmd_len += snprintf(req.command + cmd_len, sizeof(req.command) - cmd_len - 1, "%s", argv[i]);
    }

    if (parse_optional_flags(&req, argc, argv, flag_start) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
