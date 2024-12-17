#!/usr/bin/env bash

is_false_value() {
    case "$1" in
        "false"|"no"|"0")
            return 0
            ;;
        *)
            return 1  # Non-false value
            ;;
    esac
}

# Detecting the current shell and updating the corresponding configuration file
if [[ $SHELL == *"/zsh"* ]]; then
    config_file=~/.zshrc
elif [[ $SHELL == *"/bash"* ]]; then
    config_file=~/.bashrc
else
    echo "Unsupported shell. Please add 'export YB_VOYAGER_SEND_DIAGNOSTICS=$requiredEnvVarValue' manually to your shell config."
    break
fi

if [ -z "$YB_VOYAGER_SEND_DIAGNOSTICS" ] || ! is_false_value "$YB_VOYAGER_SEND_DIAGNOSTICS"; then
    echo "YB_VOYAGER_SEND_DIAGNOSTICS is not set or is not set to a recognized false value."
    while true; do
        echo "Please enter a valid false value for YB_VOYAGER_SEND_DIAGNOSTICS (\"false\", \"0\", \"no\"):"
        read requiredEnvVarValue
        if is_false_value "$requiredEnvVarValue"; then
            export YB_VOYAGER_SEND_DIAGNOSTICS=$requiredEnvVarValue
            echo "export YB_VOYAGER_SEND_DIAGNOSTICS=$requiredEnvVarValue" >> $config_file
            echo "YB_VOYAGER_SEND_DIAGNOSTICS is now set to '$requiredEnvVarValue' in your $config_file."
            echo "To make this permanent, restart your terminal or run 'source $config_file'."
            break
        else
            echo "Invalid input. The value '$requiredEnvVarValue' is not a recognized false value."
        fi
    done
else
    echo "YB_VOYAGER_SEND_DIAGNOSTICS is already set to a recognized false value: '$YB_VOYAGER_SEND_DIAGNOSTICS'."
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
SOURCE_HOOK_DIR="$REPO_ROOT/setup/hooks"
TARGET_HOOK_DIR="$REPO_ROOT/.git/hooks"

# Copy hooks and make them executable
echo "Installing git hooks..."
for hook in $(ls $SOURCE_HOOK_DIR); do
    hook_path="$TARGET_HOOK_DIR/$hook"
    # Backup existing hooks
    if [ -f "$hook_path" ]; then
        cp "$hook_path" "${hook_path}.backup"
    fi

    if cp "$SOURCE_HOOK_DIR/$hook" "$hook_path"; then
        chmod +x "$hook_path"
        echo "Installed $hook hook"
    else
        echo "Failed to install $hook hook. Check permissions or disk space."
        exit 1
    fi
done

echo "Git hooks setup completed successfully."
echo "setup.sh script complete, exiting..."