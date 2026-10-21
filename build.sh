python3.14 -m maturin build --release --locked --target x86_64-apple-darwin -i python3.14 &&
python3.14 -m maturin build --release --locked -i python3.14 &&
twine upload --repository pypi --config-file ~/.pypirc ./target/wheels/* &&
rm ./target/wheels/*
