import nx from "@nx/eslint-plugin";
import ESLINT_RULES from "@andrewt03/eslint-typescript-rules";
import eslintPluginUnicorn from "eslint-plugin-unicorn";
import globals from "globals";
import tseslint from "typescript-eslint";
import eslintSimpleImportSortPlugin from "eslint-plugin-simple-import-sort";
import eslintImportPlugin from "eslint-plugin-import";
import angularEslintPlugin from "@angular-eslint/eslint-plugin";

const DIR_NAME = import.meta.dirname;

export default [
  ...nx.configs["flat/base"],
  ...nx.configs["flat/typescript"],
  ...nx.configs["flat/javascript"],
  {
    ignores: ["**/dist"]
  },
  {
    files: ["**/*.ts", "**/*.tsx", "**/*.cts", "**/*.mts", "**/*.js", "**/*.jsx", "**/*.cjs", "**/*.mjs"],
    // Override or add rules here
    rules: {}
  },
  ...nx.configs["flat/angular"],
  ...nx.configs["flat/angular-template"],
  {
    files: ["src/**/*.ts"],
    languageOptions: {
      globals: globals.builtin,
      parser: tseslint.parser,
      parserOptions: {
        projectService: true,
        tsconfigRootDir: DIR_NAME
      }
    },
    plugins: {
      unicorn: eslintPluginUnicorn,
      "@typescript-eslint": tseslint.plugin,
      "simple-import-sort": eslintSimpleImportSortPlugin,
      import: eslintImportPlugin,
      "angular-eslint": angularEslintPlugin
    },
    rules: {
      // Standard ESLint Rules
      ...ESLINT_RULES.STANDARD_ESLINT_CONFIG_RULES,

      // TypeScript ESLint Rules
      ...ESLINT_RULES.TYPESCRIPT_ESLINT_CONFIG_RULES,

      // Unicorn ESLint Rules
      ...ESLINT_RULES.UNICORN_ESLINT_CONFIG_RULES,

      // ESLint Rules: Console/Debugger to "Warn"
      ...ESLINT_RULES.CONSOLE_DEBUGGER_WARN_ESLINT_CONFIG_RULES,

      // ESLint Rules: Sorting Imports
      ...ESLINT_RULES.SORT_IMPORT_ESLINT_CONFIG_RULES,

      // ESLint Rules: Angular
      "@angular-eslint/component-class-suffix": [
        "error",
        {
          suffixes: ["Component"]
        }
      ],
      // "@angular-eslint/component-selector": [
      //   "warn",
      //   {
      //     type: "element",
      //     prefix: "app",
      //     style: "kebab-case"
      //   }
      // ],
      "@angular-eslint/directive-class-suffix": [
        "error",
        {
          suffixes: ["Directive"]
        }
      ],
      // "@angular-eslint/directive-selector": [
      //   "warn",
      //   {
      //     type: "attribute",
      //     prefix: "app",
      //     style: "camelCase"
      //   }
      // ],
      ...ESLINT_RULES.ANGULAR_ESLINT_CONFIG_RULES
    }
  },
  {
    files: ["**/*.html"],
    // Override or add rules here
    rules: {}
  }
];
