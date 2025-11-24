# Pick version and arch
INFER_INST_VERSION=1.2.0
INFER_INST_OS_ARCH=linux-x86_64

# Download and unpack into /opt (creates /opt/infer-linux-x86_64-v1.2.0)
curl -sSL "https://github.com/facebook/infer/releases/download/v$INFER_INST_VERSION/infer-$INFER_INST_OS_ARCH-v$INFER_INST_VERSION.tar.xz" \
  | sudo tar -C /opt -xJ

# Add a convenient symlink into your PATH
sudo ln -s "/opt/infer-$INFER_INST_OS_ARCH-v$INFER_INST_VERSION/bin/infer" /usr/local/bin/infer

# Check it works
infer --version
