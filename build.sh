python3.15 -m maturin build --release --locked --target x86_64-apple-darwin -i python3.15 &&
python3.15 -m maturin build --release --locked -i python3.15 &&
twine upload --repository pypi --config-file ~/.pypirc ./target/wheels/* &&
rm ./target/wheels/*
