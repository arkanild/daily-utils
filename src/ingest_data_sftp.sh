#code in shell to process that involves copying a file from an SFTP server, and after the file has been copied, It needs to be archived  on the same SFTP server for future reference

#!/bin/bash

# Configuration variables
SFTP_USER="your_sftp_username"
SFTP_HOST="your_sftp_server"
REMOTE_SOURCE_DIR="/remote/source/directory"
REMOTE_ARCHIVE_DIR="/remote/archive/directory"
LOCAL_DEST_DIR="/local/destination/directory"
FILE_NAME="your_file_name" # You can use patterns like "*.csv" for multiple files

# Create local destination directory if it doesn't exist
mkdir -p "$LOCAL_DEST_DIR"

# Function to copy file from SFTP server to local destination
copy_file() {
  echo "Copying file from SFTP server..."
  sftp "$SFTP_USER@$SFTP_HOST" <<EOF
cd $REMOTE_SOURCE_DIR
lcd $LOCAL_DEST_DIR
get $FILE_NAME
bye
EOF

  if [[ $? -eq 0 ]]; then
    echo "File copied successfully to $LOCAL_DEST_DIR"
  else
    echo "File copy failed!" >&2
    exit 1
  fi
}

# Function to archive file on SFTP server
archive_file() {
  echo "Archiving file on SFTP server..."
  sftp "$SFTP_USER@$SFTP_HOST" <<EOF
cd $REMOTE_SOURCE_DIR
rename $FILE_NAME $REMOTE_ARCHIVE_DIR/$FILE_NAME
bye
EOF

  if [[ $? -eq 0 ]]; then
    echo "File archived successfully to $REMOTE_ARCHIVE_DIR"
  else
    echo "File archiving failed!" >&2
    exit 1
  fi
}

# Main process
copy_file
archive_file

echo "Process completed successfully."
