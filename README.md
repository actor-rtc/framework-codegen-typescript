# framework-codegen-typescript

TypeScript protoc plugin for Actor-RTC.

## Build

```bash
npm install
npm run build
```

## Usage

```bash
protoc \
  --plugin=protoc-gen-actrframework-typescript=./scripts/protoc-gen-actrframework-typescript \
  --actrframework-typescript_out=generated \
  --actrframework-typescript_opt=LocalFiles=local.proto,RemoteFiles=remote/echo.proto,RemoteFileMapping=remote/echo.proto=acme+EchoService \
  local.proto remote/echo.proto
```
