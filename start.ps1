# start.ps1

# Prompt for credentials securely
$securePassword = Read-Host "Enter DB Password" -AsSecureString
$securePassphrase = Read-Host "Enter DB Passphrase" -AsSecureString

# Convert SecureString to plain text (needed for environment variables)
$BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
$env:DB_PASSWORD = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)

$BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassphrase)
$env:DB_PASSPHRASE = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)

# Run docker-compose
docker-compose up --build