#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "========================================================="
echo "   BẮT ĐẦU SETUP MÔI TRƯỜNG AWS EC2 (UBUNTU SERVER)       "
echo "========================================================="

# 1. Cập nhật hệ thống
echo "--> 1. Đang cập nhật hệ thống..."
sudo apt update && sudo apt upgrade -y

# 2. Tạo Swap Space 2GB (Phòng ngừa lỗi Out of Memory trên t3.micro)
echo "--> 2. Đang cấu hình Swap Space 2GB..."
if [ -f /swapfile ]; then
    echo "Swapfile đã tồn tại, bỏ qua bước này."
else
    sudo fallocate -l 2G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "Cấu hình Swap Space 2GB thành công!"
fi

# 3. Cài đặt Docker
echo "--> 3. Đang cài đặt Docker..."
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common gnupg lsb-release

# Thêm khóa GPG chính thức của Docker
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Thiết lập repository ổn định của Docker
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Cấu hình để chạy Docker không cần quyền root (sudo)
echo "--> Cấu hình quyền Docker cho user hiện tại..."
sudo usermod -aG docker $USER

# 4. Cài đặt Nginx và Certbot
echo "--> 4. Đang cài đặt Nginx và Certbot (SSL)..."
sudo apt install -y nginx certbot python3-certbot-nginx

# Khởi động dịch vụ Nginx
sudo systemctl enable nginx
sudo systemctl start nginx

echo "========================================================="
echo "   SETUP HOÀN TẤT! VUI LÒNG CHẠY LỆNH SAU ĐỂ ÁP DỤNG QUYỀN DOCKER:"
echo "   newgrp docker"
echo "========================================================="
