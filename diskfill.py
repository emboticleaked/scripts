import concurrent.futures
import threading
import uuid
from time import sleep
import os

megabytes = 0
curr = 0
block_size_mb = 10  # Use a larger block size (10 MB)
files_per_gb = 100  # Each file will contain 100 blocks of 10MB = 1GB total


def allocate_space(method_name, create_file_func):
    """ Generic function to allocate space using different methods. """
    global megabytes, curr
    try:
        while True:
            target = f".___tmp_{method_name}_{uuid.uuid4()}"
            with open(target, 'wb') as f:
                create_file_func(f)  # Use the passed method to create the file
            curr += 1
    except OSError:
        print(f"No more space on the host ({method_name}).")
    except Exception as e:
        print(f"Error in {method_name}: {e}")


def method_1_direct_write(f):
    """Method 1: Directly write a large block of zeroes to the file."""
    global megabytes
    block_size = block_size_mb * 1024 * 1024  # Convert to bytes
    data = b'\x00' * block_size

    for _ in range(files_per_gb):
        f.write(data)
        megabytes += block_size_mb  # Keep track of total written data


def method_2_random_data(f):
    """Method 2: Write random data using os.urandom to the file."""
    global megabytes
    block_size = block_size_mb * 1024 * 1024  # Convert to bytes

    for _ in range(files_per_gb):
        f.write(os.urandom(block_size))
        megabytes += block_size_mb


def method_3_sparse_files(f):
    """Method 3: Create sparse files by seeking 1GB into the file."""
    global megabytes
    sparse_size = 1 * 1024 * 1024 * 1024  # 1GB sparse size
    f.seek(sparse_size - 1)  # Seek to 1GB minus 1 byte
    f.write(b'\x00')  # Write the last byte to "allocate" 1GB in a sparse way
    megabytes += sparse_size // (1024 * 1024)  # Convert to MB


def method_4_small_files(f):
    """Method 4: Write small 1KB files."""
    global megabytes
    small_size = 1 * 1024  # 1KB size
    for _ in range(1024):  # Write 1MB total with 1KB chunks
        f.write(b'\x00' * small_size)
        megabytes += 1 / 1024  # Count each KB


def display_status():
    global megabytes, curr
    displays = 0
    try:
        while True:
            sleep(0.5)  # Reduce update frequency to save CPU
            displays += 1
            print(f"Uploaded total [ {megabytes} MB ] at speed [ {curr} files/sec ]")
            if displays % 2 == 0:  # Reset every second to keep speed accurate
                curr = 0
    except Exception as e:
        print(e)


def main():
    print("Efficient storage filling using multiple methods...")
    print("Preparing...")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.submit(allocate_space, "direct_write", method_1_direct_write)
            executor.submit(allocate_space, "random_data", method_2_random_data)
            executor.submit(allocate_space, "sparse_files", method_3_sparse_files)
            executor.submit(allocate_space, "small_files", method_4_small_files)

        # Start a thread to display the status
        status_thread = threading.Thread(target=display_status)
        status_thread.start()

        # Join the status thread to keep the main program alive
        status_thread.join()

    except KeyboardInterrupt:
        print("Keyboard interrupt detected, but ignoring it...")
        while True:  # Keep the program running despite the interrupt
            sleep(1)


if __name__ == "__main__":
    main()
