import { eslintCompatPlugin } from "@oxlint/plugins";

import { noChainedTypeAssertionsRule } from "./rules/no-chained-type-assertions";
import { noConditionalEmptyArraySpreadRule } from "./rules/no-conditional-empty-array-spread";
import { noCommentsRule } from "./rules/no-comments";
import { noConditionalEmptyObjectSpreadRule } from "./rules/no-conditional-empty-object-spread";
import { noKnownValueWideningRule } from "./rules/no-known-value-widening";
import { noModuleMockingRule } from "./rules/no-module-mocking";
import { noObjectParametersRule } from "./rules/no-object-parameters";
import { noReflectApplyRule } from "./rules/no-reflect-apply";
import { noReflectGetRule } from "./rules/no-reflect-get";
import { noReprovideAmbientServiceRule } from "./rules/no-reprovide-ambient-service";
import { noRuntimeTypeofRule } from "./rules/no-runtime-typeof";
import { noForbiddenTermInSymbolNamesRule } from "./rules/no-shape-in-symbol-names";
import { noUnknownParametersRule } from "./rules/no-unknown-parameters";
import { noUnknownReturnsRule } from "./rules/no-unknown-returns";
import { noUnknownTypeAliasesRule } from "./rules/no-unknown-type-aliases";
import { noUnsafeDictionaryTypeRule } from "./rules/no-unsafe-dictionary-type";
import { noWidenThenAssertRule } from "./rules/no-widen-then-assert";
import { requireSafetyCommentForTypeAssertionRule } from "./rules/require-safety-comment-for-type-assertion";

/** Generic Oxlint rules that reject low-evidence and low-signal implementation patterns. */
const antiSlopPlugin = eslintCompatPlugin({
	meta: { name: "anti-slop" },
	rules: {
		"no-chained-type-assertions": noChainedTypeAssertionsRule,
		"no-conditional-empty-array-spread": noConditionalEmptyArraySpreadRule,
		"no-comments": noCommentsRule,
		"no-conditional-empty-object-spread": noConditionalEmptyObjectSpreadRule,
		"no-known-value-widening": noKnownValueWideningRule,
		"no-module-mocking": noModuleMockingRule,
		"no-object-parameters": noObjectParametersRule,
		"no-reflect-apply": noReflectApplyRule,
		"no-reflect-get": noReflectGetRule,
		"no-reprovide-ambient-service": noReprovideAmbientServiceRule,
		"no-runtime-typeof": noRuntimeTypeofRule,
		"no-unsafe-dictionary-type": noUnsafeDictionaryTypeRule,
		"no-shape-in-symbol-names": noForbiddenTermInSymbolNamesRule,
		"no-unknown-parameters": noUnknownParametersRule,
		"no-unknown-returns": noUnknownReturnsRule,
		"no-unknown-type-aliases": noUnknownTypeAliasesRule,
		"no-widen-then-assert": noWidenThenAssertRule,
		"require-safety-comment-for-type-assertion": requireSafetyCommentForTypeAssertionRule,
	},
});

export default antiSlopPlugin;
