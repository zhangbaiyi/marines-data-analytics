import { BaseDataSourceOptions } from "typeorm/data-source/BaseDataSourceOptions";

// Shared Typescript Types/Interfaces/Other Global-Variables Used Throughout the Project:

// Reference: https://www.totaltypescript.com/concepts/the-prettify-helper
export type Prettify<T> = {
  [K in keyof T]: T[K];
} & {};

export type Optional<T> = T | null;

// TypeORM: Entities and Migrations
export type BaseDataSourcePropertyOptions<
  T extends keyof BaseDataSourceOptions
> = NonNullable<BaseDataSourceOptions[T]>;
