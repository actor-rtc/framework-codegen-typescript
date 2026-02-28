import fs from "node:fs";
import protobuf from "protobufjs";

type FieldInfo = {
  name: string;
  number: number;
  label: number;
  type: number;
  type_name?: string;
};

type MessageInfo = {
  fullName: string;
  packageName: string;
  exportName: string;
  fields: FieldInfo[];
};

type MethodInfo = {
  serviceName: string;
  methodName: string;
  routeKey: string;
  constName: string;
  requestType: string;
  responseType: string;
  sourceFile: string;
};

type ServiceInfo = {
  fileName: string;
  packageName: string;
  name: string;
  methods: MethodInfo[];
};

type PackageData = {
  name: string;
  messages: MessageInfo[];
  services: ServiceInfo[];
};

type PluginParams = {
  localFiles: Set<string>;
  remoteFiles: Set<string>;
  remoteFileMapping: Map<string, string>;
  distImport: string;
  emitLocalActor: boolean;
};

const VERSION = "0.1.10";

const DESCRIPTOR_SCHEMA = String.raw`
syntax = "proto3";
package google.protobuf;

message FileDescriptorProto {
  string name = 1;
  string package = 2;
  repeated DescriptorProto message_type = 4;
  repeated ServiceDescriptorProto service = 6;
}

message DescriptorProto {
  string name = 1;
  repeated FieldDescriptorProto field = 2;
  repeated DescriptorProto nested_type = 3;
}

message FieldDescriptorProto {
  enum Type {
    TYPE_DOUBLE = 1;
    TYPE_FLOAT = 2;
    TYPE_INT64 = 3;
    TYPE_UINT64 = 4;
    TYPE_INT32 = 5;
    TYPE_FIXED64 = 6;
    TYPE_FIXED32 = 7;
    TYPE_BOOL = 8;
    TYPE_STRING = 9;
    TYPE_GROUP = 10;
    TYPE_MESSAGE = 11;
    TYPE_BYTES = 12;
    TYPE_UINT32 = 13;
    TYPE_ENUM = 14;
    TYPE_SFIXED32 = 15;
    TYPE_SFIXED64 = 16;
    TYPE_SINT32 = 17;
    TYPE_SINT64 = 18;
  }

  enum Label {
    LABEL_OPTIONAL = 1;
    LABEL_REQUIRED = 2;
    LABEL_REPEATED = 3;
  }

  string name = 1;
  int32 number = 3;
  Label label = 4;
  Type type = 5;
  string type_name = 6;
}

message ServiceDescriptorProto {
  string name = 1;
  repeated MethodDescriptorProto method = 2;
}

message MethodDescriptorProto {
  string name = 1;
  string input_type = 2;
  string output_type = 3;
}
`;

const COMPILER_SCHEMA = String.raw`
syntax = "proto3";
package google.protobuf.compiler;

message CodeGeneratorRequest {
  repeated string file_to_generate = 1;
  string parameter = 2;
  repeated google.protobuf.FileDescriptorProto proto_file = 15;
}

message CodeGeneratorResponse {
  string error = 1;

  message File {
    string name = 1;
    string content = 15;
  }

  repeated File file = 15;
}
`;

let codeGeneratorRequestType: protobuf.Type | null = null;
let codeGeneratorResponseType: protobuf.Type | null = null;

function ensureSchemaTypes(): {
  request: protobuf.Type;
  response: protobuf.Type;
} {
  if (codeGeneratorRequestType && codeGeneratorResponseType) {
    return { request: codeGeneratorRequestType, response: codeGeneratorResponseType };
  }

  const schemaRoot = new protobuf.Root();
  protobuf.parse(DESCRIPTOR_SCHEMA, schemaRoot, { keepCase: true });
  protobuf.parse(COMPILER_SCHEMA, schemaRoot, { keepCase: true });

  codeGeneratorRequestType = schemaRoot.lookupType(
    "google.protobuf.compiler.CodeGeneratorRequest"
  );
  codeGeneratorResponseType = schemaRoot.lookupType(
    "google.protobuf.compiler.CodeGeneratorResponse"
  );

  return { request: codeGeneratorRequestType, response: codeGeneratorResponseType };
}

function normalizePath(value: string): string {
  return value.replaceAll("\\", "/");
}

function toPascalCase(value: string): string {
  return value
    .split(/[^a-zA-Z0-9]+/g)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("");
}

function fileBaseFromPackage(packageName: string): string {
  return packageName ? packageName.replace(/\./g, "-") : "root";
}

function parseParameters(parameter: string | undefined): PluginParams {
  const params = new Map<string, string>();
  if (parameter) {
    for (const pair of parameter.split(",")) {
      if (!pair) continue;
      const idx = pair.indexOf("=");
      if (idx === -1) {
        params.set(pair.trim(), "");
      } else {
        const key = pair.slice(0, idx).trim();
        const value = pair.slice(idx + 1).trim();
        params.set(key, value);
      }
    }
  }

  const localFiles = new Set(
    (params.get("LocalFiles") ?? "")
      .split(":")
      .map((p) => p.trim())
      .filter(Boolean)
      .map(normalizePath)
  );

  const remoteFiles = new Set(
    (params.get("RemoteFiles") ?? "")
      .split(":")
      .map((p) => p.trim())
      .filter(Boolean)
      .map(normalizePath)
  );

  for (const file of localFiles) {
    if (remoteFiles.has(file)) {
      throw new Error(
        `${file}: appears in both LocalFiles and RemoteFiles; a file must belong to exactly one side.`
      );
    }
  }

  const remoteFileMapping = new Map<string, string>();
  const mappingRaw = params.get("RemoteFileMapping") ?? "";
  for (const item of mappingRaw.split(":")) {
    if (!item) continue;
    const idx = item.indexOf("=");
    if (idx <= 0 || idx === item.length - 1) {
      throw new Error(`Invalid RemoteFileMapping entry: ${item}`);
    }
    const file = normalizePath(item.slice(0, idx));
    const actrType = item.slice(idx + 1);
    remoteFileMapping.set(file, actrType);
  }

  const distImport = params.get("DistImport") || "@actor-rtc/actr";
  const emitLocalActor = (params.get("EmitLocalActor") ?? "true") !== "false";

  return {
    localFiles,
    remoteFiles,
    remoteFileMapping,
    distImport,
    emitLocalActor,
  };
}

function inferRole(
  fileName: string,
  localFiles: Set<string>,
  remoteFiles: Set<string>,
  services: unknown[]
): "local" | "remote" {
  const normalized = normalizePath(fileName);
  if (remoteFiles.has(normalized)) return "remote";
  if (localFiles.has(normalized)) return "local";
  return services.length > 0 ? "local" : "remote";
}

function exportName(packageName: string, fullName: string): string {
  const normalized = fullName.startsWith(".") ? fullName.slice(1) : fullName;
  const prefix = packageName ? `${packageName}.` : "";
  const withoutPkg = normalized.startsWith(prefix)
    ? normalized.slice(prefix.length)
    : normalized;
  const pathName = withoutPkg.replace(/\./g, "_");
  if (!packageName) return pathName;
  return `${toPascalCase(packageName)}_${pathName}`;
}

function scalarTypeName(fieldType: number): string {
  switch (fieldType) {
    case 1:
      return "double";
    case 2:
      return "float";
    case 3:
      return "int64";
    case 4:
      return "uint64";
    case 5:
      return "int32";
    case 6:
      return "fixed64";
    case 7:
      return "fixed32";
    case 8:
      return "bool";
    case 9:
      return "string";
    case 11:
      return "message";
    case 12:
      return "bytes";
    case 13:
      return "uint32";
    case 14:
      return "enum";
    case 15:
      return "sfixed32";
    case 16:
      return "sfixed64";
    case 17:
      return "sint32";
    case 18:
      return "sint64";
    default:
      return "string";
  }
}

function fieldTsType(field: FieldInfo, messageByName: Map<string, MessageInfo>): string {
  let base: string;
  const kind = scalarTypeName(field.type);

  switch (kind) {
    case "string":
      base = "string";
      break;
    case "bool":
      base = "boolean";
      break;
    case "bytes":
      base = "Buffer";
      break;
    case "int64":
    case "uint64":
    case "fixed64":
    case "sfixed64":
    case "sint64":
      base = "bigint";
      break;
    case "double":
    case "float":
    case "int32":
    case "uint32":
    case "fixed32":
    case "sfixed32":
    case "sint32":
    case "enum":
      base = "number";
      break;
    case "message": {
      const ref = field.type_name ?? "";
      const info = messageByName.get(ref);
      base = info ? info.exportName : "unknown";
      break;
    }
    default:
      base = "unknown";
  }

  if (field.label === 3) {
    return `${base}[]`;
  }
  if (kind === "message") {
    return `${base} | undefined`;
  }
  return base;
}

function collectMessages(
  packageName: string,
  parentPath: string,
  descriptors: any[],
  allMessages: MessageInfo[]
): void {
  for (const descriptor of descriptors ?? []) {
    const typePath = parentPath ? `${parentPath}.${descriptor.name}` : descriptor.name;
    const fullName = packageName ? `.${packageName}.${typePath}` : `.${typePath}`;
    const info: MessageInfo = {
      fullName,
      packageName,
      exportName: exportName(packageName, fullName),
      fields: (descriptor.field ?? []).map((field: any) => ({
        name: field.name,
        number: field.number,
        label: field.label,
        type: field.type,
        type_name: field.type_name,
      })),
    };
    allMessages.push(info);
    collectMessages(packageName, typePath, descriptor.nested_type ?? [], allMessages);
  }
}

function buildRootJson(protoFiles: any[]): any {
  const rootJson: any = { nested: {} };

  const visitMessages = (container: any, messages: any[]): void => {
    for (const msg of messages ?? []) {
      const node: any = { fields: {}, nested: {} };
      for (const field of msg.field ?? []) {
        const typeKind = scalarTypeName(field.type);
        const fieldSpec: any = {
          id: field.number,
          type:
            typeKind === "message" || typeKind === "enum"
              ? String(field.type_name || "").replace(/^\./, "")
              : typeKind,
        };
        if (field.label === 3) {
          fieldSpec.rule = "repeated";
        }
        node.fields[field.name] = fieldSpec;
      }

      visitMessages(node, msg.nested_type ?? []);
      if (Object.keys(node.nested).length === 0) {
        delete node.nested;
      }

      container.nested ??= {};
      container.nested[msg.name] = node;
    }
  };

  for (const file of protoFiles) {
    const pkg = file.package || "";
    const parts = pkg ? pkg.split(".") : [];
    let cursor = rootJson;

    for (const part of parts) {
      cursor.nested ??= {};
      cursor.nested[part] ??= { nested: {} };
      cursor = cursor.nested[part];
    }

    visitMessages(cursor, file.message_type ?? []);
  }

  return rootJson;
}

function shortTypeSuffix(exportNameValue: string): string {
  const parts = exportNameValue.split("_");
  return parts[parts.length - 1];
}

function routeConstName(serviceName: string, methodName: string, hasDuplicate: boolean): string {
  const base = hasDuplicate ? `${serviceName}_${methodName}` : methodName;
  return `${base.replace(/[^a-zA-Z0-9]/g, "_").toUpperCase()}_ROUTE_KEY`;
}

function renderPbTs(
  packageData: PackageData,
  rootJson: any,
  messageByName: Map<string, MessageInfo>
): string {
  const lines: string[] = [];
  lines.push("// DO NOT EDIT.");
  lines.push("// Generated by protoc-gen-actrframework-typescript.");
  lines.push("");
  lines.push('import protobuf from "protobufjs";');
  lines.push("");
  lines.push(`const ROOT = protobuf.Root.fromJSON(${JSON.stringify(rootJson, null, 2)} as any);`);
  lines.push("");

  for (const message of packageData.messages) {
    const lookupName = message.fullName.replace(/^\./, "");
    lines.push(`const TYPE_${message.exportName} = ROOT.lookupType("${lookupName}");`);
  }
  if (packageData.messages.length > 0) {
    lines.push("");
  }

  for (const message of packageData.messages) {
    lines.push(`export interface ${message.exportName} {`);
    for (const field of message.fields) {
      lines.push(`  ${field.name}: ${fieldTsType(field, messageByName)};`);
    }
    lines.push("}");
    lines.push("");

    lines.push(`export const ${message.exportName} = {`);
    lines.push(`  encode(message: ${message.exportName}): Buffer {`);
    lines.push(`    return Buffer.from(TYPE_${message.exportName}.encode(message as any).finish());`);
    lines.push("  },");
    lines.push("");
    lines.push(`  decode(buffer: Buffer): ${message.exportName} {`);
    lines.push(`    return TYPE_${message.exportName}.decode(buffer) as unknown as ${message.exportName};`);
    lines.push("  },");
    lines.push("};");
    lines.push("");
  }

  return lines.join("\n");
}

function renderClientTs(
  packageData: PackageData,
  messageByName: Map<string, MessageInfo>
): string {
  const lines: string[] = [];
  lines.push("// DO NOT EDIT.");
  lines.push("// Generated by protoc-gen-actrframework-typescript.");
  lines.push("");

  const methodNames = packageData.services.flatMap((service) =>
    service.methods.map((method) => method.methodName)
  );
  const hasDuplicateMethod = methodNames.length !== new Set(methodNames).size;

  const pbFile = `./${fileBaseFromPackage(packageData.name)}.pb`;
  const imports = new Set<string>();
  for (const service of packageData.services) {
    for (const method of service.methods) {
      const req = messageByName.get(method.requestType);
      const res = messageByName.get(method.responseType);
      if (req) imports.add(req.exportName);
      if (res) imports.add(res.exportName);
    }
  }

  if (imports.size > 0) {
    lines.push(`import { ${Array.from(imports).sort().join(", ")} } from "${pbFile}";`);
    lines.push("");
  }

  for (const service of packageData.services) {
    for (const method of service.methods) {
      const constName = routeConstName(service.name, method.methodName, hasDuplicateMethod);
      lines.push(`export const ${constName} = "${method.routeKey}";`);
    }
  }
  if (packageData.services.length > 0) {
    lines.push("");
  }

  for (const service of packageData.services) {
    for (const method of service.methods) {
      const req = messageByName.get(method.requestType);
      const res = messageByName.get(method.responseType);
      if (!req || !res) continue;

      if (req.fields.length === 1) {
        const field = req.fields[0];
        lines.push(
          `export function encode${shortTypeSuffix(req.exportName)}(value: ${fieldTsType(
            field,
            messageByName
          )}): Buffer {`
        );
        lines.push(`  return ${req.exportName}.encode({ ${field.name}: value } as ${req.exportName});`);
        lines.push("}");
      } else {
        lines.push(`export function encode${shortTypeSuffix(req.exportName)}(message: ${req.exportName}): Buffer {`);
        lines.push(`  return ${req.exportName}.encode(message);`);
        lines.push("}");
      }
      lines.push("");

      lines.push(`export function decode${shortTypeSuffix(res.exportName)}(buffer: Buffer): ${res.exportName} {`);
      lines.push(`  return ${res.exportName}.decode(buffer);`);
      lines.push("}");
      lines.push("");
    }
  }

  return lines.join("\n");
}

function parseActrType(value: string): { manufacturer: string; name: string } | null {
  const idx = value.indexOf("+");
  if (idx <= 0 || idx === value.length - 1) return null;
  return {
    manufacturer: value.slice(0, idx),
    name: value.slice(idx + 1),
  };
}

function renderLocalActorTs(
  routeEntries: Array<{
    constName: string;
    clientFile: string;
    targetType: { manufacturer: string; name: string };
  }>,
  distImport: string
): string {
  const lines: string[] = [];
  lines.push("// DO NOT EDIT.");
  lines.push("// Generated by protoc-gen-actrframework-typescript.");
  lines.push("");
  lines.push(`import type { Context, RpcEnvelope, PayloadType } from "${distImport}";`);

  const imports = new Map<string, string[]>();
  for (const entry of routeEntries) {
    if (!imports.has(entry.clientFile)) {
      imports.set(entry.clientFile, []);
    }
    imports.get(entry.clientFile)!.push(entry.constName);
  }

  for (const [file, constNames] of imports.entries()) {
    const uniq = Array.from(new Set(constNames)).sort();
    lines.push(`import { ${uniq.join(", ")} } from "${file}";`);
  }
  lines.push("");

  lines.push("const RPC_TIMEOUT_MS = 15000;");
  lines.push("const RPC_PAYLOAD_TYPE: PayloadType = 0;");
  lines.push("");
  lines.push("const ROUTES = [");
  for (const entry of routeEntries) {
    lines.push("  {");
    lines.push(`    routeKey: ${entry.constName},`);
    lines.push(`    targetType: ${JSON.stringify(entry.targetType)},`);
    lines.push("  },");
  }
  lines.push("];");
  lines.push("");

  lines.push("export async function dispatchLocalActor(");
  lines.push("  ctx: Context,");
  lines.push("  envelope: RpcEnvelope");
  lines.push("): Promise<Buffer> {");
  lines.push("  const match = ROUTES.find((route) => route.routeKey === envelope.routeKey);");
  lines.push("  if (!match) {");
  lines.push("    throw new Error(`Unknown route: ${envelope.routeKey}`);");
  lines.push("  }");
  lines.push("");
  lines.push("  const targetId = await ctx.discover(match.targetType);");
  lines.push("  return await ctx.callRaw(");
  lines.push("    targetId,");
  lines.push("    envelope.routeKey,");
  lines.push("    RPC_PAYLOAD_TYPE,");
  lines.push("    envelope.payload,");
  lines.push("    RPC_TIMEOUT_MS");
  lines.push("  );");
  lines.push("}");

  return lines.join("\n");
}

function buildResponseFiles(requestObj: any): Array<{ name: string; content: string }> {
  const params = parseParameters(requestObj.parameter);

  const fileByName = new Map<string, any>();
  for (const file of requestObj.proto_file ?? []) {
    fileByName.set(normalizePath(file.name), file);
  }

  const packages = new Map<string, PackageData>();
  const allMessages: MessageInfo[] = [];
  const remoteServicesByFile = new Map<string, ServiceInfo[]>();

  for (const fileNameRaw of requestObj.file_to_generate ?? []) {
    const fileName = normalizePath(fileNameRaw);
    const file = fileByName.get(fileName);
    if (!file) continue;

    const packageName = file.package || "";
    if (!packages.has(packageName)) {
      packages.set(packageName, { name: packageName, messages: [], services: [] });
    }
    const pkg = packages.get(packageName)!;

    const fileMessages: MessageInfo[] = [];
    collectMessages(packageName, "", file.message_type ?? [], fileMessages);
    pkg.messages.push(...fileMessages);
    allMessages.push(...fileMessages);

    const role = inferRole(fileName, params.localFiles, params.remoteFiles, file.service ?? []);

    const methodNames = (file.service ?? []).flatMap((service: any) =>
      (service.method ?? []).map((method: any) => method.name)
    );
    const hasDuplicateMethod = methodNames.length !== new Set(methodNames).size;

    const remoteServices: ServiceInfo[] = [];
    for (const service of file.service ?? []) {
      const serviceInfo: ServiceInfo = {
        fileName,
        packageName,
        name: service.name,
        methods: [],
      };

      for (const method of service.method ?? []) {
        serviceInfo.methods.push({
          serviceName: service.name,
          methodName: method.name,
          routeKey: packageName
            ? `${packageName}.${service.name}.${method.name}`
            : `${service.name}.${method.name}`,
          constName: routeConstName(service.name, method.name, hasDuplicateMethod),
          requestType: method.input_type,
          responseType: method.output_type,
          sourceFile: fileName,
        });
      }

      pkg.services.push(serviceInfo);
      if (role === "remote") {
        remoteServices.push(serviceInfo);
      }
    }

    if (remoteServices.length > 0) {
      remoteServicesByFile.set(fileName, remoteServices);
    }
  }

  const messageByName = new Map<string, MessageInfo>();
  for (const msg of allMessages) {
    messageByName.set(msg.fullName, msg);
  }

  const rootJson = buildRootJson(requestObj.proto_file ?? []);

  const outputFiles: Array<{ name: string; content: string }> = [];
  for (const pkg of packages.values()) {
    if (pkg.messages.length > 0) {
      outputFiles.push({
        name: `${fileBaseFromPackage(pkg.name)}.pb.ts`,
        content: renderPbTs(pkg, rootJson, messageByName),
      });
    }
    if (pkg.services.length > 0) {
      outputFiles.push({
        name: `${fileBaseFromPackage(pkg.name)}.client.ts`,
        content: renderClientTs(pkg, messageByName),
      });
    }
  }

  if (params.emitLocalActor) {
    const mappingTypes = Array.from(params.remoteFileMapping.values())
      .map(parseActrType)
      .filter((v): v is { manufacturer: string; name: string } => v !== null);
    const fallbackType = mappingTypes.length === 1 ? mappingTypes[0] : null;

    const routeEntries: Array<{
      constName: string;
      clientFile: string;
      targetType: { manufacturer: string; name: string };
    }> = [];

    for (const [fileName, services] of remoteServicesByFile.entries()) {
      const explicit = parseActrType(params.remoteFileMapping.get(fileName) || "");
      const targetType = explicit ?? fallbackType;
      if (!targetType) {
        throw new Error(
          `No actr_type mapping found for remote file ${fileName}. Use RemoteFileMapping=remote/path.proto=manufacturer+Service.`
        );
      }

      for (const service of services) {
        for (const method of service.methods) {
          routeEntries.push({
            constName: method.constName,
            clientFile: `./${fileBaseFromPackage(service.packageName)}.client`,
            targetType,
          });
        }
      }
    }

    if (routeEntries.length > 0) {
      outputFiles.push({
        name: "local.actor.ts",
        content: renderLocalActorTs(routeEntries, params.distImport),
      });
    }
  }

  const dedup = new Map<string, string>();
  for (const file of outputFiles) {
    dedup.set(file.name, file.content);
  }

  return Array.from(dedup.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, content]) => ({ name, content }));
}

function writeResponse(responseObj: any, responseType: protobuf.Type): void {
  const encoded = responseType.encode(responseObj).finish();
  fs.writeFileSync(1, Buffer.from(encoded));
}

function main(): void {
  const args = process.argv.slice(2);
  if (args.includes("--version") || args.includes("-V")) {
    process.stdout.write(`protoc-gen-actrframework-typescript ${VERSION}\n`);
    return;
  }
  if (args.includes("--help") || args.includes("-h")) {
    process.stdout.write(
      "protoc-gen-actrframework-typescript - Protobuf plugin for Actor-RTC TypeScript framework\n\n" +
        "USAGE:\n" +
        "  protoc --plugin=protoc-gen-actrframework-typescript=PATH --actrframework-typescript_out=OUT_DIR input.proto\n" +
        "  protoc-gen-actrframework-typescript --version\n"
    );
    return;
  }

  const { request: requestType, response: responseType } = ensureSchemaTypes();

  try {
    const input = fs.readFileSync(0);
    const requestMessage = requestType.decode(input);
    const requestObj = requestType.toObject(requestMessage, {
      defaults: true,
      arrays: true,
      objects: true,
      longs: Number,
    });

    const files = buildResponseFiles(requestObj);
    writeResponse({ file: files }, responseType);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    writeResponse({ error: message }, responseType);
  }
}

main();
