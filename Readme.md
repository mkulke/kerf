# kerfuffle

rust rpc playground

## build

```
brew install protobuf
cargo build
```

## run

```
./target/debug/server -l 127.0.0.1:1234 -c 127.0.0.1:2345 127.0.0.1:3456
listening on 127.0.0.1:1234
```

```
./target/debug/client -s 127.0.0.1:1234 -c 127.0.0.1:3456
Client received: true
```

