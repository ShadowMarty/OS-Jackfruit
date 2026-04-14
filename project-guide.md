# Multi-Container Runtime

**Team Size:** 2 Students

---

### Project Summary

This project involves building a lightweight Linux container runtime in C with a long-running parent supervisor and a kernel-space memory monitor. The container runtime must manage multiple containers at once, coordinate concurrent logging safely, expose a small supervisor CLI, and include controlled experiments related to Linux scheduling.

The project has two integrated parts:

1. **User-Space Runtime + Supervisor (`engine.c`)**  
   Launches and manages multiple isolated containers, maintains metadata for each container, accepts CLI commands, captures container output through a bounded-buffer logging system, and handles container lifecycle signals correctly.
2. **Kernel-Space Monitor (`monitor.c`)**  
   Implements a Linux Kernel Module (LKM) that tracks container processes, enforces soft and hard memory limits, and integrates with the user-space runtime through `ioctl`.

---

### Environment and Setup

The project is designed for this environment:

- **Ubuntu 22.04 or 24.04 in a VM**
- **Secure Boot OFF** for module loading
- **No WSL**

Install dependencies:

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

This repository also includes a minimal GitHub Actions smoke check that is copied into student forks. It is intentionally limited to CI-safe steps on a hosted runner and does **not** replace VM-based testing.

The smoke check expects this command to succeed:

```bash
make -C boilerplate ci
```

That target should build only the user-space binaries needed for a quick compile check and should not require `sudo`, kernel headers, module loading, rootfs setup, or a running supervisor.

Run the environment preflight check before implementation:

```bash
cd boilerplate
chmod +x environment-check.sh
sudo ./environment-check.sh
```

Prepare the Alpine mini root filesystem:

```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base
```

Create one writable rootfs copy per container before launch:

```bash
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
```

No need to keep `rootfs-base/` or per-container `rootfs-*` directories in GitHub repo.

---

### Architecture Overview

The runtime is a single binary (`engine`) used in two ways:

1. **Supervisor daemon** — started once with `engine supervisor ./rootfs-base`. It stays alive, manages containers, and owns the logging pipeline.
2. **CLI client** — each command like `engine start alpha ./rootfs-alpha /bin/sh` is a short-lived process that sends a request to the running supervisor and prints the response.

The CLI process connects to the supervisor over an IPC control channel, sends a command, receives a response, and exits. The supervisor, upon receiving a `start` command, calls `clone()` to create a new container child process with its own namespaces. Each container's stdout and stderr are connected back to the supervisor via pipes, which feed into the bounded-buffer logging pipeline.

There are two separate IPC paths in this project:

- **Path A (logging):** Container stdout/stderr → Supervisor, via pipes. Described in Task 3.
- **Path B (control):** CLI process → Supervisor, via a UNIX domain socket, FIFO, or shared-memory channel. Described in Task 2.

### CLI Contract

Use this exact command interface:

```bash
engine supervisor <base-rootfs>
engine start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
engine run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
engine ps
engine logs <id>
engine stop <id>
```

A few semantics:

- `<container-rootfs>` must be unique per running container (no two live containers may share the same writable rootfs directory)
- If `--soft-mib`/`--hard-mib` are omitted, default to `40 MiB` soft and `64 MiB` hard
- `start` returns after the supervisor accepts the request and records metadata
- `run` blocks until that container exits, then returns the container exit status (`exit_code`, or `128 + signal` if signaled)
- `run` uses the same logging pipeline and log files as `start`; live terminal streaming is optional
- If the `run` client receives `SIGINT`/`SIGTERM`, it must forward termination intent to the supervisor (equivalent to `stop <id>`) and continue waiting for final status

---

### Implementation Scope

#### Task 1: Multi-Container Runtime with Parent Supervisor

Implement a parent supervisor process that can manage multiple containers at the same time instead of launching only one shell and exiting.

Demonstrate:

- Supervisor process remains alive while containers run
- Multiple containers can be started and tracked concurrently
- Each container has isolated PID, UTS, and mount namespaces
- Each container uses its own rootfs copy derived from the provided base rootfs
- `/proc` works correctly inside each container
- Parent reaps exited children correctly with no zombies

**Filesystem isolation:** Each container needs its own root filesystem view. Treat `rootfs-base/` as the template and run each container with a separate writable copy (for example `rootfs-alpha/`, `rootfs-beta/`). Do not run multiple live containers against the same writable rootfs directory. Use `chroot` (simpler) or `pivot_root` (more thorough — prevents escape via `..` traversal) to make the container see only its assigned `container-rootfs` directory as `/`. Inside the container, mount `/proc` so that tools like `ps` work:

```c
mount("proc", "/proc", "proc", 0, NULL);
```

To run helper binaries (e.g., test workloads) inside a container, copy them into that container's rootfs before launch (or copy into `rootfs-base` before creating per-container copies):

```bash
cp workload_binary ./rootfs-alpha/
```

For each container, the supervisor must maintain metadata in user space. At minimum track:

- Container ID or name
- Host PID
- Start time
- Current state (`starting`, `running`, `stopped`, `killed`, etc.)
- Configured soft and hard memory limits
- Log file path
- Exit status or terminating signal after completion

The internal representation is up to you to design, but it must be safe under concurrent access.

#### Task 2: Supervisor CLI and Signal Handling

Implement a CLI interface for interacting with the supervisor.

The command grammar and semantics in **Canonical CLI Contract (Required)** are mandatory.

Required commands:

- `start` to launch a new container in the background
- `run` to launch a container and wait for it in the foreground
- `ps` to list tracked containers and their metadata
- `logs` to inspect a container log file
- `stop` to terminate a running container cleanly

You may add more commands if you want, but the above are required.

Demonstrate:

- CLI requests reach the long-running supervisor correctly
- Supervisor updates container state after each command
- `SIGCHLD` handling is correct and does not leak zombies
- `SIGINT`/`SIGTERM` to the supervisor trigger orderly shutdown
- Container termination path distinguishes graceful stop vs forced kill

This task covers **Path B (control):** the IPC channel between the CLI client process and the supervisor daemon (see Architecture Overview). This channel must use a different IPC mechanism than the logging pipes in Task 3. A UNIX domain socket, FIFO, or shared-memory-based command channel are all acceptable if justified.

The CLI process sends a command string (e.g., `start alpha ./rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80`) over this channel. The supervisor reads the command, acts on it, and sends a response back. Design the message format so the supervisor can parse the command type, required arguments, and optional flags.

#### Task 3: Bounded-Buffer Logging and IPC Design

This task covers **Path A (logging):** the pipe-based IPC from each container's stdout/stderr into the supervisor (see Architecture Overview).

Capture container output through a concurrent logging pipeline rather than writing directly to the terminal.

Demonstrate:

- File-descriptor-based IPC from each container into the supervisor
- Capture of both `stdout` and `stderr`
- A bounded buffer in user space between producers and consumers
- Correct producer-consumer synchronization
- Persistent per-container log files
- Clean logger shutdown when containers exit or the supervisor stops

Minimum concurrency expectation:

- At least one producer thread reads container output from pipes and inserts log data into a bounded shared buffer
- At least one consumer thread removes data from the buffer and writes to log files
- Shared metadata access is synchronized separately from the log buffer

**Correctness properties you must demonstrate:**

1. No log lines are dropped when a container exits abruptly
2. The buffer does not deadlock when it is full and a container is trying to log
3. Consumer threads observe a termination signal and flush all remaining entries before exiting

**Design for cleanup now:** Your producer threads must exit cleanly when their container exits. Your consumer threads must be joinable. Do not defer this to Task 6 — if threads are not designed to shut down from the start, bolting cleanup on later will not work.

Your README must explain:

- Why you chose your synchronization primitives
- What race conditions exist without them
- How your bounded buffer avoids lost data, corruption, or deadlock

#### Task 4: Kernel Memory Monitoring with Soft and Hard Limits

Extend the kernel monitor beyond a single kill-on-limit policy.

Demonstrate:

- Control device at `/dev/container_monitor`
- PID registration from the supervisor via `ioctl`
- Tracking of monitored processes in a kernel linked list
- Lock-protected shared list access (`mutex` or `spinlock`)
- Periodic RSS checks
- Separate soft-limit and hard-limit behavior
- Removal of stale or exited entries

Required policy behavior:

- **Soft limit:** log a warning event when the process first exceeds the soft limit
- **Hard limit:** terminate the process when it exceeds the hard limit

Integration detail:

- The supervisor must send the container's **host PID** to the kernel module
- The user-space metadata must reflect whether a container exited normally, was stopped by the supervisor, or was killed due to the hard limit

Required attribution rule for grading consistency:

- The supervisor must set an internal `stop_requested` flag before signaling a container from `stop`
- Classify termination as `stopped` when `stop_requested` is set and the container exits due to that stop flow
- Classify termination as `hard_limit_killed` only when the exit signal is `SIGKILL` and `stop_requested` is not set
- Keep the final reason in metadata so `ps` output can distinguish normal exit, manual stop, and hard-limit kill

Exact event-reporting design is open to you. You may choose a simple `dmesg`-only design, or you may add a user-space notification path if you can justify it clearly.

#### Task 5: Scheduler Experiments and Analysis

Use the runtime to run controlled experiments that connect the project to Linux scheduling behavior.

Demonstrate:

- At least two concurrent workloads with different behavior, such as CPU-bound and I/O-bound processes
- At least two scheduling configurations, such as different `nice` values or CPU affinities
- Measurement of observable outcomes such as completion time, responsiveness, or CPU share
- A short analysis of how the Linux scheduler treated the workloads

The goal is not to reimplement a scheduler. The goal is to use your runtime as an experimental platform and explain scheduling behavior using evidence.

At least one experiment must compare:

- Two containers running CPU-bound work with different priorities, or
- A CPU-bound container and an I/O-bound container running at the same time

#### Task 6: Resource Cleanup

By this point, cleanup logic should already be built into Tasks 1–4. This task is about verifying and demonstrating that teardown works end-to-end, not about designing it from scratch.

Verify clean teardown in both user and kernel space:

- Child process reap in the supervisor (designed in Task 1)
- Logging threads exit and join correctly (designed in Task 3)
- File descriptors are closed on all paths
- User-space heap resources are released
- Kernel list entries are freed on module unload (designed in Task 4)
- No lingering zombie processes or stale metadata after demo run

---

### Engineering Analysis

In your `README.md`, include an analysis section that connects your implementation to OS fundamentals. This is not a description of what you coded - it is an explanation of _why the OS works this way_ and how your project exercises those mechanisms.

Address these five areas:

1. **Isolation Mechanisms**  
   How does your runtime achieve process and filesystem isolation? Explain the role of namespaces (PID, UTS, mount) and `chroot`/`pivot_root` at the kernel level. What does the host kernel still share with all containers?

2. **Supervisor and Process Lifecycle**  
   Why is a long-running parent supervisor useful here? Explain process creation, parent-child relationships, reaping, metadata tracking, and signal delivery across the container lifecycle.

3. **IPC, Threads, and Synchronization**  
   Your project uses at least two IPC mechanisms and a bounded-buffer logging design. For each shared data structure, identify the possible race conditions and justify your synchronization choice (`mutex`, `condition variable`, `semaphore`, `spinlock`, etc.).

4. **Memory Management and Enforcement**  
   Explain what RSS measures and what it does not measure. Why are soft and hard limits different policies? Why does the enforcement mechanism belong in kernel space rather than only in user space?

5. **Scheduling Behavior**  
   Use your experiment results to explain how Linux scheduling affected your workloads. Relate your results to scheduling goals such as fairness, responsiveness, and throughput.

### Boilerplate Contents

The `boilerplate/` folder will contain starter files for the runtime, kernel monitor, shared `ioctl` definitions, test workloads, and build flow so that you have structure for starting out.

---

### Submission Package

Submit a GitHub repository containing:

#### Source Files

We expect you to copy the boilerplate and work on top of that, but you may then structure the repository however you want, but the following must be present:

1. `engine.c` — user-space runtime and supervisor
2. `monitor.c` — kernel-space memory monitor (LKM)
3. `monitor_ioctl.h` — shared ioctl definitions between user and kernel space
4. At least two workload/test programs used for memory and scheduling demonstrations
5. `Makefile` — must support building all of the above with a single `make`
6. `README.md`

For the inherited GitHub Actions smoke check, keep a CI-safe build path available through:

```bash
make -C boilerplate ci
```

This is only for quick user-space compilation checks on GitHub-hosted runners. Your full project must still build and run in the required Ubuntu VM environment.

#### `README.md`

**1. Team Information**

- Team member names with SRNs

**2. Build, Load, and Run Instructions**

- Step-by-step commands to build the project, load the kernel module, and start the supervisor
- How to launch containers, run workloads, and use the CLI
- How to unload the module and clean up
- These must be complete enough that we can reproduce your setup from scratch on a fresh Ubuntu 22.04/24.04 VM

The following is a reference run sequence you can use as a starting point:

```bash
# Build
make

# Load kernel module
sudo insmod monitor.ko

# Verify control device
ls -l /dev/container_monitor

# Start supervisor
sudo ./engine supervisor ./rootfs-base

# Create per-container writable rootfs copies
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta

# In another terminal: start two containers
sudo ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80
sudo ./engine start beta ./rootfs-beta /bin/sh --soft-mib 64 --hard-mib 96

# List tracked containers
sudo ./engine ps

# Inspect one container
sudo ./engine logs alpha

# Run memory test inside a container
# (copy the test program into rootfs before launch if needed)

# Run scheduling experiment workloads
# and compare observed behavior

# Stop containers
sudo ./engine stop alpha
sudo ./engine stop beta

# Stop supervisor if your design keeps it separate

# Inspect kernel logs
dmesg | tail

# Unload module
sudo rmmod monitor
```

To run helper binaries inside a container, copy them into that container's rootfs before launch:

```bash
cp workload_binary ./rootfs-alpha/
```

**3. Demo with Screenshots**

You must provide annotated screenshots that demonstrate each of the following. Each screenshot must include a very brief caption explaining what it shows.

| #   | What to Demonstrate         | What the Screenshot Must Show                                                                                                             |
| --- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Multi-container supervision | Two or more containers running under one supervisor process                                                                               |
| 2   | Metadata tracking           | Output of the `ps` command showing tracked container metadata                                                                             |
| 3   | Bounded-buffer logging      | Log file contents captured through the logging pipeline, and evidence of the pipeline operating (e.g., producer/consumer activity)        |
| 4   | CLI and IPC                 | A CLI command being issued and the supervisor responding, demonstrating the second IPC mechanism                                          |
| 5   | Soft-limit warning          | `dmesg` or log output showing a soft-limit warning event for a container                                                                  |
| 6   | Hard-limit enforcement      | `dmesg` or log output showing a container being killed after exceeding its hard limit, and the supervisor metadata reflecting the kill    |
| 7   | Scheduling experiment       | Terminal output or measurements from at least one scheduling experiment, with observable differences between configurations               |
| 8   | Clean teardown              | Evidence that containers are reaped, threads exit, and no zombies remain after shutdown (e.g., `ps aux` output, supervisor exit messages) |

**4. Engineering Analysis**

Address the five areas described in the [Engineering Analysis](#engineering-analysis) section above. This is not a description of what you coded — it is an explanation of _why the OS works this way_ and how your project exercises those mechanisms.

**5. Design Decisions and Tradeoffs**

For each major subsystem (namespace isolation, supervisor architecture, IPC/logging, kernel monitor, scheduling experiments), explain:

- The design choice you made
- One concrete tradeoff of that choice
- Your justification for why it was the right call

**6. Scheduler Experiment Results**

- Present raw data or measurements from your experiments
- Include at least one comparison (e.g., table or side-by-side output)
- Explain what the results show about Linux scheduling behavior
