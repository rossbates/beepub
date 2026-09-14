import { get } from "./client";
import type { SyncCapabilitiesOut } from "$lib/types";

export const syncApi = {
  capabilities: () =>
    get("/sync/capabilities") as Promise<SyncCapabilitiesOut>,
};
