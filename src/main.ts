import type { DescFile, DescMethod, DescService } from "@bufbuild/protobuf";
import {
  createEcmaScriptPlugin,
  runNodeJs,
  type Schema,
} from "@bufbuild/protoplugin";

type PluginParams = {
  localFiles: Set<string>;
  remoteFiles: Set<string>;
  remoteFileMapping: Map<string, string>;
  distImport: string;
  emitLocalActor: boolean;
};

type RemoteDispatchEntry = {
  requestCompanionName: string;
  clientImportPath: string;
  targetType: {
    manufacturer: string;
    name: string;
  };
};

type AnalyzedMethod = {
  service: DescService;
  method: DescMethod;
  helperSuffix: string;
  requestCompanionName: string;
};

type LocalMethodEntry = {
  requestCompanionName: string;
  handlerName: string;
  method: DescMethod;
};

type MethodMetadata = {
  name: string;
  snake_name: string;
  input_type: string;
  output_type: string;
  route_key: string;
};

type LocalServiceMetadata = {
  name: string;
  package: string;
  proto_file: string;
  handler_interface: string;
  workload_type: string;
  dispatcher_type: string;
  methods: MethodMetadata[];
};

type RemoteServiceMetadata = {
  name: string;
  package: string;
  proto_file: string;
  actr_type: string;
  client_type: string;
  methods: MethodMetadata[];
};

const VERSION = "0.1.11";

const plugin = createEcmaScriptPlugin<PluginParams>({
  name: "protoc-gen-actrframework-typescript",
  version: VERSION,
  parseOptions: parseOptions,
  generateTs(schema) {
    generateTypeScript(schema);
  },
});

runNodeJs(plugin);

function generateTypeScript(schema: Schema<PluginParams>): void {
  const remoteDispatchEntries: RemoteDispatchEntry[] = [];
  const localMethods: LocalMethodEntry[] = [];
  const localServiceMetadata: LocalServiceMetadata[] = [];
  const remoteServiceMetadata: RemoteServiceMetadata[] = [];
  const fallbackTargetType = buildFallbackTargetType(
    schema.options.remoteFileMapping,
  );
  const requestOwners = new Map<string, string>();
  const localCompanionNames = new Set<string>();

  for (const file of schema.files) {
    if (file.services.length === 0) {
      continue;
    }

    const role = inferRole(file, schema.options);
    const analyzedMethods = analyzeFileMethods(file);
    validateUniqueRequestBindings(requestOwners, analyzedMethods);

    if (role !== "remote") {
      localServiceMetadata.push(
        ...buildLocalServiceMetadata(file, analyzedMethods),
      );
      for (const entry of analyzedMethods) {
        if (localCompanionNames.has(entry.requestCompanionName)) {
          throw new Error(
            `Local request companion '${entry.requestCompanionName}' is duplicated. ` +
              "Each local RPC request type name must be unique.",
          );
        }
        localCompanionNames.add(entry.requestCompanionName);
      }
      localMethods.push(...buildLocalMethodEntries(analyzedMethods));
      continue;
    }

    generateClientFile(schema, file, analyzedMethods);

    const explicitTargetType = parseActrType(
      schema.options.remoteFileMapping.get(normalizeProtoFileKey(file.name)) ??
        "",
    );
    const targetType = explicitTargetType ?? fallbackTargetType;
    if (!targetType) {
      throw new Error(
        `No actr_type mapping found for remote file ${file.name}. ` +
          "Use RemoteFileMapping=remote/path.proto=manufacturer+Service.",
      );
    }

    remoteServiceMetadata.push(
      ...buildRemoteServiceMetadata(file, analyzedMethods, targetType),
    );

    remoteDispatchEntries.push(
      ...buildRemoteDispatchEntries(file, targetType, analyzedMethods),
    );
  }

  if (
    schema.options.emitLocalActor &&
    (localMethods.length > 0 || remoteDispatchEntries.length > 0)
  ) {
    generateLocalActorFile(schema, localMethods, remoteDispatchEntries);
  }

  const metadataFile = schema.generateFile("actr-gen-meta.json");
  metadataFile.print(
    JSON.stringify(
      {
        plugin_version: VERSION,
        language: "typescript",
        local_services: localServiceMetadata,
        remote_services: remoteServiceMetadata,
      },
      null,
      2,
    ),
  );
}

function generateClientFile(
  schema: Schema<PluginParams>,
  file: DescFile,
  analyzedMethods: AnalyzedMethod[],
): void {
  const generated = schema.generateFile(
    `${normalizeProtoFileKey(file.name)}_client.ts`,
  );
  generated.preamble(file);
  const bufferSymbol = generated.import("Buffer", "node:buffer");

  const toBinary = generated.import("toBinary", "@bufbuild/protobuf");
  const fromBinary = generated.import("fromBinary", "@bufbuild/protobuf");
  const create = generated.import("create", "@bufbuild/protobuf");
  const messageInitShape = generated.import(
    "MessageInitShape",
    "@bufbuild/protobuf",
    true,
  );

  for (const entry of analyzedMethods) {
    const requestType = generated.importShape(entry.method.input);
    const requestSchema = generated.importSchema(entry.method.input);
    const responseType = generated.importShape(entry.method.output);
    const responseSchema = generated.importSchema(entry.method.output);

    generated.print(
      generated.export("const", entry.requestCompanionName),
      " = {",
    );
    generated.print(
      "  routeKey: ",
      generated.string(routeKeyForMethod(entry.method)),
      ",",
    );
    generated.print(
      "  encode(message: ",
      messageInitShape,
      "<typeof ",
      requestSchema,
      ">): ",
      bufferSymbol,
      " {",
    );
    generated.print(
      "    return ",
      bufferSymbol,
      ".from(",
      toBinary,
      "(",
      requestSchema,
      ", ",
      create,
      "(",
      requestSchema,
      ", message)));",
    );
    generated.print("  },");
    generated.print("  decode(bytes: Uint8Array): ", requestType, " {");
    generated.print("    return ", fromBinary, "(", requestSchema, ", bytes);");
    generated.print("  },");
    generated.print("  response: {");
    generated.print(
      "    encode(message: ",
      messageInitShape,
      "<typeof ",
      responseSchema,
      ">): ",
      bufferSymbol,
      " {",
    );
    generated.print(
      "      return ",
      bufferSymbol,
      ".from(",
      toBinary,
      "(",
      responseSchema,
      ", ",
      create,
      "(",
      responseSchema,
      ", message)));",
    );
    generated.print("    },");
    generated.print("    decode(bytes: Uint8Array): ", responseType, " {");
    generated.print(
      "      return ",
      fromBinary,
      "(",
      responseSchema,
      ", bytes);",
    );
    generated.print("    },");
    generated.print("  },");
    generated.print("} as const;");
    generated.print("");
  }
}

function generateLocalActorFile(
  schema: Schema<PluginParams>,
  localMethods: LocalMethodEntry[],
  remoteDispatchEntries: RemoteDispatchEntry[],
): void {
  const generated = schema.generateFile("local_actor.ts");
  const bufferSymbol = generated.import("Buffer", "node:buffer");

  const contextType = generated.import(
    "Context",
    schema.options.distImport,
    true,
  );
  const envelopeType = generated.import(
    "RpcEnvelope",
    schema.options.distImport,
    true,
  );
  const payloadType = generated.import(
    "PayloadType",
    schema.options.distImport,
    true,
  );

  generated.print("const RPC_TIMEOUT_MS = 15000;");
  generated.print("const RPC_PAYLOAD_TYPE: ", payloadType, " = 0;");
  generated.print("");

  if (localMethods.length > 0) {
    const toBinary = generated.import("toBinary", "@bufbuild/protobuf");
    const fromBinary = generated.import("fromBinary", "@bufbuild/protobuf");
    const create = generated.import("create", "@bufbuild/protobuf");
    const messageInitShape = generated.import(
      "MessageInitShape",
      "@bufbuild/protobuf",
      true,
    );

    for (const entry of localMethods) {
      const requestType = generated.importShape(entry.method.input);
      const requestSchema = generated.importSchema(entry.method.input);
      const responseType = generated.importShape(entry.method.output);
      const responseSchema = generated.importSchema(entry.method.output);

      generated.print(
        generated.export("const", entry.requestCompanionName),
        " = {",
      );
      generated.print(
        "  routeKey: ",
        generated.string(routeKeyForMethod(entry.method)),
        ",",
      );
      generated.print(
        "  encode(message: ",
        messageInitShape,
        "<typeof ",
        requestSchema,
        ">): ",
        bufferSymbol,
        " {",
      );
      generated.print(
        "    return ",
        bufferSymbol,
        ".from(",
        toBinary,
        "(",
        requestSchema,
        ", ",
        create,
        "(",
        requestSchema,
        ", message)));",
      );
      generated.print("  },");
      generated.print("  decode(bytes: Uint8Array): ", requestType, " {");
      generated.print(
        "    return ",
        fromBinary,
        "(",
        requestSchema,
        ", bytes);",
      );
      generated.print("  },");
      generated.print("  response: {");
      generated.print(
        "    encode(message: ",
        messageInitShape,
        "<typeof ",
        responseSchema,
        ">): ",
        bufferSymbol,
        " {",
      );
      generated.print(
        "      return ",
        bufferSymbol,
        ".from(",
        toBinary,
        "(",
        responseSchema,
        ", ",
        create,
        "(",
        responseSchema,
        ", message)));",
      );
      generated.print("    },");
      generated.print("    decode(bytes: Uint8Array): ", responseType, " {");
      generated.print(
        "      return ",
        fromBinary,
        "(",
        responseSchema,
        ", bytes);",
      );
      generated.print("    },");
      generated.print("  },");
      generated.print("} as const;");
      generated.print("");
    }

    generated.print(generated.export("type", "LocalHandlers"), " = {");
    for (const entry of localMethods) {
      const requestType = generated.importShape(entry.method.input);
      const responseSchema = generated.importSchema(entry.method.output);
      generated.print(
        "  ",
        entry.handlerName,
        ": (request: ",
        requestType,
        ", ctx: ",
        contextType,
        ") => ",
        messageInitShape,
        "<typeof ",
        responseSchema,
        "> | Promise<",
        messageInitShape,
        "<typeof ",
        responseSchema,
        ">>;",
      );
    }
    generated.print("};");
  } else {
    generated.print(generated.export("type", "LocalHandlers"), " = {};");
  }
  generated.print("");

  generated.print(
    generated.export("async function", "dispatchLocalActor"),
    "(ctx: ",
    contextType,
    ", envelope: ",
    envelopeType,
    ", handlers?: LocalHandlers",
    "): Promise<",
    bufferSymbol,
    "> {",
  );
  if (localMethods.length > 0) {
    generated.print("  switch (envelope.routeKey) {");
    for (const entry of localMethods) {
      generated.print("    case ", entry.requestCompanionName, ".routeKey: {");
      generated.print(
        "      const handler = handlers?.",
        entry.handlerName,
        ";",
      );
      generated.print("      if (!handler) {");
      generated.print(
        "        throw new Error(`Local handler ",
        entry.handlerName,
        " is not configured for route ${envelope.routeKey}`);",
      );
      generated.print("      }");
      generated.print(
        "      const request = ",
        entry.requestCompanionName,
        ".decode(envelope.payload);",
      );
      generated.print("      const response = await handler(request, ctx);");
      generated.print(
        "      return ",
        entry.requestCompanionName,
        ".response.encode(response);",
      );
      generated.print("    }");
    }
  } else {
    generated.print("  switch (envelope.routeKey) {");
  }

  for (const entry of remoteDispatchEntries) {
    const routeSymbol = generated.import(
      entry.requestCompanionName,
      `./${entry.clientImportPath}`,
    );
    generated.print("    case ", routeSymbol, ".routeKey: {");
    generated.print(
      "      const targetId = await ctx.discover({ manufacturer: ",
      generated.string(entry.targetType.manufacturer),
      ", name: ",
      generated.string(entry.targetType.name),
      " });",
    );
    generated.print("      return await ctx.callRaw(");
    generated.print("        targetId,");
    generated.print("        envelope.routeKey,");
    generated.print("        RPC_PAYLOAD_TYPE,");
    generated.print("        envelope.payload,");
    generated.print("        RPC_TIMEOUT_MS");
    generated.print("      );");
    generated.print("    }");
  }
  generated.print("    default:");
  generated.print(
    "      throw new Error(`Unknown route: ${envelope.routeKey}`);",
  );
  generated.print("  }");
  generated.print("}");
}

function buildRemoteDispatchEntries(
  file: DescFile,
  targetType: {
    manufacturer: string;
    name: string;
  },
  analyzedMethods: AnalyzedMethod[],
): RemoteDispatchEntry[] {
  return analyzedMethods.map((entry) => ({
    requestCompanionName: entry.requestCompanionName,
    clientImportPath: `${normalizeProtoFileKey(file.name)}_client`,
    targetType,
  }));
}

function buildLocalMethodEntries(
  analyzedMethods: AnalyzedMethod[],
): LocalMethodEntry[] {
  return analyzedMethods.map((entry) => ({
    requestCompanionName: entry.requestCompanionName,
    handlerName: `handle${entry.helperSuffix}`,
    method: entry.method,
  }));
}

function buildFallbackTargetType(
  remoteFileMapping: Map<string, string>,
): { manufacturer: string; name: string } | null {
  const mappingTypes = Array.from(remoteFileMapping.values())
    .map(parseActrType)
    .filter(
      (value): value is { manufacturer: string; name: string } =>
        value !== null,
    );
  return mappingTypes.length === 1 ? mappingTypes[0] : null;
}

function parseOptions(
  rawOptions: { key: string; value: string }[],
): PluginParams {
  const localFiles = new Set<string>();
  const remoteFiles = new Set<string>();
  const remoteFileMapping = new Map<string, string>();
  let distImport = "@actor-rtc/actr";
  let emitLocalActor = true;

  for (const { key, value } of rawOptions) {
    switch (key) {
      case "LocalFiles":
        appendPathList(localFiles, value);
        break;
      case "RemoteFiles":
        appendPathList(remoteFiles, value);
        break;
      case "RemoteFileMapping":
        appendRemoteMapping(remoteFileMapping, value);
        break;
      case "DistImport":
        if (!value) {
          throw new Error("DistImport cannot be empty.");
        }
        distImport = value;
        break;
      case "EmitLocalActor":
        emitLocalActor = parseBooleanOption("EmitLocalActor", value);
        break;
      default:
        throw new Error(`Unknown option '${key}'.`);
    }
  }

  for (const file of localFiles) {
    if (remoteFiles.has(file)) {
      throw new Error(
        `${file}: appears in both LocalFiles and RemoteFiles; a file must belong to exactly one side.`,
      );
    }
  }

  return {
    localFiles,
    remoteFiles,
    remoteFileMapping,
    distImport,
    emitLocalActor,
  };
}

function appendPathList(target: Set<string>, rawValue: string): void {
  for (const item of rawValue.split(":")) {
    const normalized = normalizeProtoFileKey(item.trim());
    if (normalized) {
      target.add(normalized);
    }
  }
}

function appendRemoteMapping(
  target: Map<string, string>,
  rawValue: string,
): void {
  for (const item of rawValue.split(":")) {
    const trimmed = item.trim();
    if (!trimmed) {
      continue;
    }
    const idx = trimmed.indexOf("=");
    if (idx <= 0 || idx === trimmed.length - 1) {
      throw new Error(`Invalid RemoteFileMapping entry: ${trimmed}`);
    }
    const file = normalizeProtoFileKey(trimmed.slice(0, idx));
    const actrType = trimmed.slice(idx + 1);
    target.set(file, actrType);
  }
}

function parseBooleanOption(name: string, value: string): boolean {
  switch (value) {
    case "":
    case "true":
    case "1":
      return true;
    case "false":
    case "0":
      return false;
    default:
      throw new Error(`${name} must be true/false or 1/0.`);
  }
}

function inferRole(file: DescFile, params: PluginParams): "local" | "remote" {
  const normalized = normalizeProtoFileKey(file.name);
  if (params.remoteFiles.has(normalized)) {
    return "remote";
  }
  if (params.localFiles.has(normalized)) {
    return "local";
  }
  return file.services.length > 0 ? "local" : "remote";
}

function analyzeFileMethods(file: DescFile): AnalyzedMethod[] {
  const methodNames = file.services.flatMap((service) =>
    service.methods.map((method) => method.name),
  );
  const hasDuplicateMethod = methodNames.length !== new Set(methodNames).size;
  const analyzedMethods: AnalyzedMethod[] = [];
  const companionNames = new Set<string>();

  for (const service of file.services) {
    for (const method of service.methods) {
      if (method.methodKind !== "unary") {
        throw new Error(
          `${service.typeName}.${method.name}: only unary RPC methods are supported for TypeScript generation.`,
        );
      }

      const requestCompanionName = requestCompanionNameForMethod(method);
      if (companionNames.has(requestCompanionName)) {
        throw new Error(
          `${service.typeName}.${method.name}: request companion '${requestCompanionName}' is duplicated within ${file.name}. ` +
            "Each request type name must map to exactly one RPC in the generated file.",
        );
      }
      companionNames.add(requestCompanionName);

      analyzedMethods.push({
        service,
        method,
        helperSuffix: helperSuffix(service, method, hasDuplicateMethod),
        requestCompanionName,
      });
    }
  }

  return analyzedMethods;
}

function buildLocalServiceMetadata(
  file: DescFile,
  analyzedMethods: AnalyzedMethod[],
): LocalServiceMetadata[] {
  return groupAnalyzedMethodsByService(analyzedMethods).map(
    ([service, methods]) => ({
      name: service.name,
      package: packageNameForService(service),
      proto_file: normalizePath(file.name),
      handler_interface: "LocalHandlers",
      workload_type: "Workload",
      dispatcher_type: "dispatchLocalActor",
      methods: methods.map((entry) => buildMethodMetadata(entry.method)),
    }),
  );
}

function buildRemoteServiceMetadata(
  file: DescFile,
  analyzedMethods: AnalyzedMethod[],
  targetType: { manufacturer: string; name: string },
): RemoteServiceMetadata[] {
  return groupAnalyzedMethodsByService(analyzedMethods).map(
    ([service, methods]) => ({
      name: service.name,
      package: packageNameForService(service),
      proto_file: normalizePath(file.name),
      actr_type: `${targetType.manufacturer}+${targetType.name}`,
      client_type: `${service.name}Client`,
      methods: methods.map((entry) => buildMethodMetadata(entry.method)),
    }),
  );
}

function groupAnalyzedMethodsByService(
  analyzedMethods: AnalyzedMethod[],
): Array<[DescService, AnalyzedMethod[]]> {
  const grouped = new Map<DescService, AnalyzedMethod[]>();
  for (const entry of analyzedMethods) {
    const existing = grouped.get(entry.service);
    if (existing) {
      existing.push(entry);
    } else {
      grouped.set(entry.service, [entry]);
    }
  }
  return Array.from(grouped.entries());
}

function buildMethodMetadata(method: DescMethod): MethodMetadata {
  return {
    name: method.name,
    snake_name: toSnakeCase(method.name),
    input_type: method.input.name,
    output_type: method.output.name,
    route_key: routeKeyForMethod(method),
  };
}

function packageNameForService(service: DescService): string {
  const suffix = `.${service.name}`;
  if (service.typeName.endsWith(suffix)) {
    return service.typeName.slice(0, -suffix.length);
  }
  return "";
}

function validateUniqueRequestBindings(
  requestOwners: Map<string, string>,
  analyzedMethods: AnalyzedMethod[],
): void {
  for (const entry of analyzedMethods) {
    const requestTypeName = entry.method.input.typeName;
    const routeKey = routeKeyForMethod(entry.method);
    const owner = requestOwners.get(requestTypeName);
    if (owner && owner !== routeKey) {
      throw new Error(
        `${routeKey}: request type ${requestTypeName} is already bound to ${owner}. ` +
          "TypeScript strong-associated generation requires each request type to map to exactly one RPC.",
      );
    }
    requestOwners.set(requestTypeName, routeKey);
  }
}

function requestCompanionNameForMethod(method: DescMethod): string {
  return method.input.name;
}

function helperSuffix(
  service: DescService,
  method: DescMethod,
  hasDuplicateMethod: boolean,
): string {
  const base = hasDuplicateMethod
    ? `${service.name}_${method.name}`
    : method.name;
  return toPascalCase(base);
}

function routeKeyForMethod(method: DescMethod): string {
  return `${method.parent.typeName}.${method.name}`;
}

function parseActrType(
  value: string,
): { manufacturer: string; name: string } | null {
  const idx = value.indexOf("+");
  if (idx <= 0 || idx === value.length - 1) {
    return null;
  }
  return {
    manufacturer: value.slice(0, idx),
    name: value.slice(idx + 1),
  };
}

function normalizePath(value: string): string {
  return value.replaceAll("\\", "/");
}

function normalizeProtoFileKey(value: string): string {
  const normalized = normalizePath(value);
  return normalized.endsWith(".proto")
    ? normalized.slice(0, -".proto".length)
    : normalized;
}

function toPascalCase(value: string): string {
  return value
    .split(/[^a-zA-Z0-9]+/g)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("");
}

function toSnakeCase(value: string): string {
  return value.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
}
