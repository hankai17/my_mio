curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
curl https://sh.rustup.rs -sSf | sh -s -- --help
source "$HOME/.cargo/env"
rustc --version
cargo --version

#rustup install nightly
#rustup default nightly
#cargo +nightly build
