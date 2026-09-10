import { eslintCompatPlugin } from "@oxlint/plugins";

import { noManualEffectErrorTagRule } from "./rules/no-manual-effect-error-tag.js";
import { noManualTagComparisonRule } from "./rules/no-manual-tag-comparison.js";
import { noManualTaggedConstructionRule } from "./rules/no-manual-tagged-construction.js";
import { noServiceConstructorImportsRule } from "./rules/no-service-constructor-imports.js";
import { preferEffectMatchRule } from "./rules/prefer-effect-match.js";

/** Opt-in Oxlint rules for Effect service and Layer architecture. */
const antiSlopEffectPlugin = eslintCompatPlugin({
	meta: { name: "anti-slop-effect" },
	rules: {
		"no-manual-effect-error-tag": noManualEffectErrorTagRule,
		"no-manual-tag-comparison": noManualTagComparisonRule,
		"no-manual-tagged-construction": noManualTaggedConstructionRule,
		"no-service-constructor-imports": noServiceConstructorImportsRule,
		"prefer-effect-match": preferEffectMatchRule,
	},
});

export default antiSlopEffectPlugin;
