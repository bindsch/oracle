declare module "win-dpapi" {
  export type Scope = "CurrentUser" | "LocalMachine";

  export interface DpapiModule {
    unprotectData(encrypted: Buffer, optionalEntropy: Buffer | null, scope: Scope): Buffer;
  }

  const dpapi: DpapiModule;
  export = dpapi;
}
