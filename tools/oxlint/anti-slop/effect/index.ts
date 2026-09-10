import { eslintCompatPlugin } from "@oxlint/plugins";

import { noManualEffectErrorTagRule } from "./rules/no-manual-effect-error-tag";
import { noManualTagComparisonRule } from "./rules/no-manual-tag-comparison";
import { noManualTaggedConstructionRule } from "./rules/no-manual-tagged-construction";
import { noServiceConstructorImportsRule } from "./rules/no-service-constructor-imports";
import { preferEffectMatchRule } from "./rules/prefer-effect-match";

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
