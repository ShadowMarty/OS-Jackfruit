name: Submission Smoke Check

on:
  push:
  pull_request:

jobs:
  smoke:
    runs-on: ubuntu-24.04

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install build tools
        run: |
          sudo apt-get update
          sudo apt-get install -y build-essential

      - name: Build CI-safe user-space targets
        run: make -C boilerplate ci

      - name: Check engine usage path
        run: |
          set +e
          ./boilerplate/engine >engine.out 2>&1
          rc=$?
          if [ "$rc" -eq 0 ]; then
            echo "./boilerplate/engine unexpectedly exited with status 0"
            exit 1
          fi
          if ! grep -q "Usage:" engine.out; then
            echo "Expected usage output from ./boilerplate/engine"
            cat engine.out
            exit 1
          fi
