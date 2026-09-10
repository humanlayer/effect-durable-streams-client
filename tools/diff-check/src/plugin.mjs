import { tsImport } from "tsx/esm/api";

export default (await tsImport("./plugin", import.meta.url)).default;
