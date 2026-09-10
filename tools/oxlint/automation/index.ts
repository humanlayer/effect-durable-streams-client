import noBannedTypeAssertions from "./rules/no-banned-type-assertions";
import noCascadingLayerProvide from "./rules/no-cascading-layer-provide";
import noApiBackendImports from "./rules/no-api-backend-imports";
import noApiRepositoryImports from "./rules/no-api-repository-imports";
import noAmbientNondeterminism from "./rules/no-ambient-nondeterminism";
import noComments from "./rules/no-comments";
import noCssModules from "./rules/no-css-modules";
import noDirectBrowserStorage from "./rules/no-direct-browser-storage";
import noDirectFetch from "./rules/no-direct-fetch";
import noDirectXstateCreateMachine from "./rules/no-direct-xstate-create-machine";
import noDirectXstateUseSelector from "./rules/no-direct-xstate-use-selector";
import noDisableValidation from "./rules/no-disable-validation";
import noEffectAsvoid from "./rules/no-effect-asvoid";
import noGlobalJson from "./rules/no-global-json";
import noInOperator from "./rules/no-in-operator";
import noJsxStyle from "./rules/no-jsx-style";
import noFixedHeightOnContent from "./rules/no-fixed-height-on-content";
import noMultipleFunctionParams from "./rules/no-multiple-function-params";
import noMultipleXstateHooks from "./rules/no-multiple-xstate-hooks";
import noNestedEffectArrayMethods from "./rules/no-nested-effect-array-methods";
import noNestedLayerProvide from "./rules/no-nested-layer-provide";
import noNonRouteLayoutFiles from "./rules/no-non-route-layout-files";
import noOptionalFunctionParameters from "./rules/no-optional-function-parameters";
import noReexportOnlyModules from "./rules/no-reexport-only-modules";
import noReactComponentInnerFunctions from "./rules/no-react-component-inner-functions";
import noReactNonComponentFunctionExports from "./rules/no-react-non-component-function-exports";
import noReactStateHooks from "./rules/no-react-state-hooks";
import noServiceOption from "./rules/no-service-option";
import noShadowedStandardArrayStatic from "./rules/no-shadowed-standard-array-static";
import noSilentErrorSwallow from "./rules/no-silent-error-swallow";
import noStaticEffectServiceForwarders from "./rules/no-static-effect-service-forwarders";
import noSingleUsePrivateFunctions from "./rules/no-single-use-private-functions";
import noSingleUseXstateActions from "./rules/no-single-use-xstate-actions";
import noSingleUseXstateGuards from "./rules/no-single-use-xstate-guards";
import noSqlTypeParameter from "./rules/no-sql-type-parameter";
import noSwitch from "./rules/no-switch";
import noTypeAssertion from "./rules/no-type-assertion";
import noTypeofObject from "./rules/no-typeof-object";
import noTryCatch from "./rules/no-try-catch";
import pipeMaxArguments from "./rules/pipe-max-arguments";
import preferEffectMatch from "./rules/prefer-effect-match";
import preferOptionFromNullable from "./rules/prefer-option-from-nullable";
import preferTaggedErrorHandling from "./rules/prefer-tagged-error-handling";
import privateFunctionPrefix from "./rules/private-function-prefix";
import requireApprovedUiPrimitives from "./rules/require-approved-ui-primitives";
import requireCnForClassnameComposition from "./rules/require-cn-for-classname-composition";
import requireContextServiceInServices from "./rules/require-context-service-in-services";
import requireDefaultComponentExport from "./rules/require-default-component-export";
import requireTsxInUiFolders from "./rules/require-tsx-in-ui-folders";
import requireXstateEventSatisfies from "./rules/require-xstate-event-satisfies";
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

export { allRules, profile, profiles } from "./profiles";
export {
  oxlintRuleMetadata,
  rulesByProfile,
  severityByRule,
} from "./catalog";
