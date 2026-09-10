import noBannedTypeAssertions from "./rules/no-banned-type-assertions.js";
import noCascadingLayerProvide from "./rules/no-cascading-layer-provide.js";
import noApiBackendImports from "./rules/no-api-backend-imports.js";
import noApiRepositoryImports from "./rules/no-api-repository-imports.js";
import noAmbientNondeterminism from "./rules/no-ambient-nondeterminism.js";
import noComments from "./rules/no-comments.js";
import noCssModules from "./rules/no-css-modules.js";
import noDirectBrowserStorage from "./rules/no-direct-browser-storage.js";
import noDirectFetch from "./rules/no-direct-fetch.js";
import noDirectXstateCreateMachine from "./rules/no-direct-xstate-create-machine.js";
import noDirectXstateUseSelector from "./rules/no-direct-xstate-use-selector.js";
import noDisableValidation from "./rules/no-disable-validation.js";
import noEffectAsvoid from "./rules/no-effect-asvoid.js";
import noGlobalJson from "./rules/no-global-json.js";
import noInOperator from "./rules/no-in-operator.js";
import noJsxStyle from "./rules/no-jsx-style.js";
import noFixedHeightOnContent from "./rules/no-fixed-height-on-content.js";
import noMultipleFunctionParams from "./rules/no-multiple-function-params.js";
import noMultipleXstateHooks from "./rules/no-multiple-xstate-hooks.js";
import noNestedEffectArrayMethods from "./rules/no-nested-effect-array-methods.js";
import noNestedLayerProvide from "./rules/no-nested-layer-provide.js";
import noNonRouteLayoutFiles from "./rules/no-non-route-layout-files.js";
import noOptionalFunctionParameters from "./rules/no-optional-function-parameters.js";
import noReexportOnlyModules from "./rules/no-reexport-only-modules.js";
import noReactComponentInnerFunctions from "./rules/no-react-component-inner-functions.js";
import noReactNonComponentFunctionExports from "./rules/no-react-non-component-function-exports.js";
import noReactStateHooks from "./rules/no-react-state-hooks.js";
import noServiceOption from "./rules/no-service-option.js";
import noShadowedStandardArrayStatic from "./rules/no-shadowed-standard-array-static.js";
import noSilentErrorSwallow from "./rules/no-silent-error-swallow.js";
import noStaticEffectServiceForwarders from "./rules/no-static-effect-service-forwarders.js";
import noSingleUsePrivateFunctions from "./rules/no-single-use-private-functions.js";
import noSingleUseXstateActions from "./rules/no-single-use-xstate-actions.js";
import noSingleUseXstateGuards from "./rules/no-single-use-xstate-guards.js";
import noSqlTypeParameter from "./rules/no-sql-type-parameter.js";
import noSwitch from "./rules/no-switch.js";
import noTypeAssertion from "./rules/no-type-assertion.js";
import noTypeofObject from "./rules/no-typeof-object.js";
import noTryCatch from "./rules/no-try-catch.js";
import pipeMaxArguments from "./rules/pipe-max-arguments.js";
import preferEffectMatch from "./rules/prefer-effect-match.js";
import preferOptionFromNullable from "./rules/prefer-option-from-nullable.js";
import preferTaggedErrorHandling from "./rules/prefer-tagged-error-handling.js";
import privateFunctionPrefix from "./rules/private-function-prefix.js";
import requireApprovedUiPrimitives from "./rules/require-approved-ui-primitives.js";
import requireCnForClassnameComposition from "./rules/require-cn-for-classname-composition.js";
import requireContextServiceInServices from "./rules/require-context-service-in-services.js";
import requireDefaultComponentExport from "./rules/require-default-component-export.js";
import requireTsxInUiFolders from "./rules/require-tsx-in-ui-folders.js";
import requireXstateEventSatisfies from "./rules/require-xstate-event-satisfies.js";
import type { RuleTester } from "oxlint/plugins-dev";

type Rule = Parameters<RuleTester["run"]>[1];

interface Plugin {
  readonly meta: {
    readonly name: string;
  };
  readonly rules: Readonly<Record<string, Rule>>;
}

const plugin: Plugin = {
  meta: {
    name: "automation",
  },
  rules: {
    "no-ambient-nondeterminism": noAmbientNondeterminism,
    "no-api-backend-imports": noApiBackendImports,
    "no-api-repository-imports": noApiRepositoryImports,
    "no-banned-type-assertions": noBannedTypeAssertions,
    "no-cascading-layer-provide": noCascadingLayerProvide,
    "no-comments": noComments,
    "no-css-modules": noCssModules,
    "no-direct-browser-storage": noDirectBrowserStorage,
    "no-direct-fetch": noDirectFetch,
    "no-direct-xstate-create-machine": noDirectXstateCreateMachine,
    "no-direct-xstate-use-selector": noDirectXstateUseSelector,
    "no-disable-validation": noDisableValidation,
    "no-effect-asvoid": noEffectAsvoid,
    "no-global-json": noGlobalJson,
    "no-in-operator": noInOperator,
    "no-jsx-style": noJsxStyle,
    "no-fixed-height-on-content": noFixedHeightOnContent,
    "no-multiple-function-params": noMultipleFunctionParams,
    "no-multiple-xstate-hooks": noMultipleXstateHooks,
    "no-nested-effect-array-methods": noNestedEffectArrayMethods,
    "no-nested-layer-provide": noNestedLayerProvide,
    "no-non-route-layout-files": noNonRouteLayoutFiles,
    "no-optional-function-parameters": noOptionalFunctionParameters,
    "no-reexport-only-modules": noReexportOnlyModules,
    "no-react-component-inner-functions": noReactComponentInnerFunctions,
    "no-react-non-component-function-exports":
      noReactNonComponentFunctionExports,
    "no-react-state-hooks": noReactStateHooks,
    "no-service-option": noServiceOption,
    "no-shadowed-standard-array-static": noShadowedStandardArrayStatic,
    "no-silent-error-swallow": noSilentErrorSwallow,
    "no-static-effect-service-forwarders": noStaticEffectServiceForwarders,
    "no-single-use-private-functions": noSingleUsePrivateFunctions,
    "no-single-use-xstate-actions": noSingleUseXstateActions,
    "no-single-use-xstate-guards": noSingleUseXstateGuards,
    "no-sql-type-parameter": noSqlTypeParameter,
    "no-switch": noSwitch,
    "no-type-assertion": noTypeAssertion,
    "no-typeof-object": noTypeofObject,
    "no-try-catch": noTryCatch,
    "pipe-max-arguments": pipeMaxArguments,
    "prefer-effect-match": preferEffectMatch,
    "prefer-option-from-nullable": preferOptionFromNullable,
    "prefer-tagged-error-handling": preferTaggedErrorHandling,
    "private-function-prefix": privateFunctionPrefix,
    "require-approved-ui-primitives": requireApprovedUiPrimitives,
    "require-cn-for-classname-composition": requireCnForClassnameComposition,
    "require-context-service-in-services": requireContextServiceInServices,
    "require-default-component-export": requireDefaultComponentExport,
    "require-tsx-in-ui-folders": requireTsxInUiFolders,
    "require-xstate-event-satisfies": requireXstateEventSatisfies,
  },
};

export default plugin;

export { allRules, profile, profiles } from "./profiles.js";
export {
  oxlintRuleMetadata,
  rulesByProfile,
  severityByRule,
} from "./catalog.js";
