# kerfuffle

rust rpc playground

## build

```
brew install protobuf
cargo build
```

## run

```
./target/debug/server
listening on 127.0.0.1:55976
Press ENTER to exit...
```

```
./target/debug/client 55976 Alice
Client received: Hello Alice!
```

