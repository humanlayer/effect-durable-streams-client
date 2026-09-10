import { tsImport } from "tsx/esm/api";

export default (await tsImport("./plugin.js", import.meta.url)).default;
