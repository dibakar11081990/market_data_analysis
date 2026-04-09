import paramiko
import tempfile
import os

def fetch_process_sftp_file(
    sftp_host,
    sftp_port,
    username,
    password,
    remote_path
):
    # Step 1: Setup SFTP connection
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Step 2: Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_local_path = temp_file.name
        print(f"Downloading SFTP file to temp file: {temp_local_path}")

        # Step 3: Download the file to the temporary file
        sftp.get(remote_path, temp_local_path)

    try:
        # Step 4: Do your processing here
        with open(temp_local_path, 'r') as f:
            data = f.read()
            print("File content (truncated):", data[:100])  # example processing

        # Step 5: Optionally move it, upload to S3, etc.

    finally:
        # Step 6: Cleanup - delete the temporary file
        if os.path.exists(temp_local_path):
            os.remove(temp_local_path)
            print(f"Temp file deleted: {temp_local_path}")

        # Step 7: Close the SFTP connection
        sftp.close()
        transport.close()

# 🔁 Example usage
fetch_process_sftp_file(
    sftp_host="sftp.example.com",
    sftp_port=22,
    username="your_user",
    password="your_pass",
    remote_path="/remote/path/to/data.csv"
)
