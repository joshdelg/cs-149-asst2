#!/bin/bash

# List of test cases without "async"
tests=(
    "simple_test_sync"
    "ping_pong_equal"
    "ping_pong_unequal"
    "super_light"
    "super_super_light"
    "recursive_fibonacci"
    "math_operations_in_tight_for_loop"
    "math_operations_in_tight_for_loop_fewer_tasks"
    "math_operations_in_tight_for_loop_fan_in"
    "math_operations_in_tight_for_loop_reduction_tree"
    "spin_between_run_calls"
    "mandelbrot_chunked"
    "simple_run_deps_test"
)

# Binaries
binaries=("./runtasks" "./runtasks_ref_linux")

# Loop over each test and run it on both binaries
for test in "${tests[@]}"; do
    echo "Running test: $test"
    for binary in "${binaries[@]}"; do
        echo "Using binary: $binary"
        $binary $test
    done
done

