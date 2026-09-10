import { tsImport } from "tsx/esm/api";

export default (await tsImport("./index", import.meta.url)).default;
