# Copy blog posts to WSL
Copy-Item -Path "C:\Users\AlterUser\Documents\git\notes\Vault\7 - Blog" -Destination "\\wsl.localhost\Ubuntu-24.04\home\carlos\projects\blog\content\posts\" -Recurse -Force

# Run Hugo in WSL
wsl -d Ubuntu-24.04 -e bash -c "source ~/.zshrc && cd ~/projects/blog && hugo"

# Copy generated files back to Windows
Copy-Item -Path "\\wsl.localhost\Ubuntu-24.04\home\carlos\projects\blog\public" -Destination "C:\Users\AlterUser\Documents\git\blog\public\" -Recurse -Force

# Git operations
Set-Location "C:\Users\AlterUser\Documents\git\blog"
git add .
$commitMessage = "New Blog Post on $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
git commit -m "$commitMessage"
git push