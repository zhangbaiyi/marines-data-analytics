#!/bin/bash

# Update Homebrew
echo "Updating Homebrew..."
brew update

# Install RabbitMQ
echo "Installing RabbitMQ..."
brew install rabbitmq

# Starting RabbitMQ in the background
echo "Starting RabbitMQ service..."
brew services start rabbitmq

# Enable all feature flags (recommended)
echo "Enabling all feature flags on RabbitMQ..."
/opt/homebrew/sbin/rabbitmqctl enable_feature_flag all

# Verify RabbitMQ installation
echo "Verifying RabbitMQ installation..."
brew services list
rabbitmqctl status

# Print completion status
echo "RabbitMQ installation completed successfully on macOS!"
