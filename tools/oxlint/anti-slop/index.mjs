import { tsImport } from "tsx/esm/api";

export default (await tsImport("./index.js", import.meta.url)).default;
