import { AuthProvider, DataProvider } from 'ra-core'
import { createClient } from '@clickhouse/client-web'
import type { ResponseJSON, ClickHouseClientConfigOptions, ClickHouseClient } from '@clickhouse/client-web'

let clickhouse: ClickHouseClient

export function sql(strings: TemplateStringsArray) {
  return (queryParams: Record<string, any> = {}) => {
    return clickhouse?.query({
      query: strings[0],
      // format: 'JSON',
      query_params: queryParams,
    }) //.then((res) => res.json()) as Promise<ResponseJSON>
  }
}

export const ClickhouseDataProvider = (config: ClickHouseClientConfigOptions): DataProvider => {
    if (!clickhouse) clickhouse = createClient(config)
    return {
        // getList: async (resource, params) => {
        //     const { page = 1, perPage = 25 } = params.pagination || { }
        //     const offset = (page - 1) * perPage
        //     const result = await sql` SELECT * FROM {resource: String} LIMIT {perPage: UInt32} OFFSET {offset: UInt32} `({ resource, perPage, offset })
        //     result.
        //     return { data: result.data, total: result.meta.total }
        getList: async (resource, params) => {
          const { page, perPage } = params.pagination || { page: 1, perPage: 25 }
          const { field, order } = params.sort || { field: 'id', order: 'ASC' }
          const filters = params.filter;
      
          const offset = (page - 1) * perPage;
      
          // Build the WHERE clause from filters
          const whereClauses = Object.entries(filters).map(([key, value]) => `${key} = '${value}'`).join(' AND ');
          const whereClause = whereClauses ? `WHERE ${whereClauses}` : '';
      
          const combinedQuery = `
            WITH
              (SELECT count() FROM ${resource} ${whereClause}) AS total_count
            SELECT *, total_count
            FROM ${resource}
            ${whereClause}
            ORDER BY ${field} ${order}
            LIMIT ${perPage} OFFSET ${offset}
          `;
      
          const response = await clickhouse.query({
            query: combinedQuery,
            format: 'JSON',
          })
      
          const { data } = await response.json();
          const total = data.length > 0 ? data[0].total_count : 0;
      
          // Remove the total_count field from the returned data
          const resultData = data.map(item => {
            const { total_count, ...rest } = item;
            return rest;
          });
      
          return {
            data: resultData,
            total: parseInt(total, 10),
          };
        },
      
        getOne: async (resource, params) => {
          const query = `
            SELECT *
            FROM ${resource}
            WHERE id = ${params.id}
            LIMIT 1
          `;
      
          const response = await clickhouse.query({
            query,
            format: 'JSONEachRow',
          }).toPromise();
      
          const data = await response.json();
      
          return {
            data: data[0],
          };
        },
      
        getMany: async (resource, params) => {
          const ids = params.ids.join(',');
      
          const query = `
            SELECT *
            FROM ${resource}
            WHERE id IN (${ids})
          `;
      
          const response = await clickhouse.query({
            query,
            format: 'JSONEachRow',
          }).toPromise();
      
          const data = await response.json();
      
          return {
            data,
          };
        },
      
        getManyReference: async (resource, params) => {
          const { page, perPage } = params.pagination;
          const { field, order } = params.sort;
          const { target, id } = params;
      
          const offset = (page - 1) * perPage;
      
          const whereClause = `${target} = ${id}`;
      
          const combinedQuery = `
            WITH
              (SELECT count() FROM ${resource} WHERE ${whereClause}) AS total_count
            SELECT *, total_count
            FROM ${resource}
            WHERE ${whereClause}
            ORDER BY ${field} ${order}
            LIMIT ${perPage} OFFSET ${offset}
          `;
      
          const response = await clickhouse.query({
            query: combinedQuery,
            format: 'JSONEachRow',
          }).toPromise();
      
          const data = await response.json();
          const total = data.length > 0 ? data[0].total_count : 0;
      
          // Remove the total_count field from the returned data
          const resultData = data.map(item => {
            const { total_count, ...rest } = item;
            return rest;
          });
      
          return {
            data: resultData,
            total: parseInt(total, 10),
          };
        },
      
        create: async (resource, params) => {
          const keys = Object.keys(params.data).join(',');
          const values = Object.values(params.data).map(value => `'${value}'`).join(',');
      
          const query = `
            INSERT INTO ${resource} (${keys})
            VALUES (${values})
          `;
      
          await clickhouse.query({
            query,
          }).toPromise();
      
          return { data: { ...params.data, id: Date.now() } };
        },
      
        update: async (resource, params) => {
          const updates = Object.entries(params.data)
            .map(([key, value]) => `${key} = '${value}'`)
            .join(',');
      
          const query = `
            ALTER TABLE ${resource}
            UPDATE ${updates}
            WHERE id = ${params.id}
          `;
      
          await clickhouse.query({
            query,
          }).toPromise();
      
          return { data: params.data };
        },
      
        updateMany: async (resource, params) => {
          const updates = Object.entries(params.data)
            .map(([key, value]) => `${key} = '${value}'`)
            .join(',');
      
          const ids = params.ids.join(',');
      
          const query = `
            ALTER TABLE ${resource}
            UPDATE ${updates}
            WHERE id IN (${ids})
          `;
      
          await clickhouse.query({
            query,
          }).toPromise();
      
          return { data: params.ids };
        },
      
        delete: async (resource, params) => {
          const query = `
            ALTER TABLE ${resource}
            DELETE
            WHERE id = ${params.id}
          `;
      
          await clickhouse.query({
            query,
          }).toPromise();
      
          return { data: params.previousData };
        },
      
        deleteMany: async (resource, params) => {
          const ids = params.ids.join(',');
      
          const query = `
            ALTER TABLE ${resource}
            DELETE
            WHERE id IN (${ids})
          `;
      
          await clickhouse.query({
            query,
          }).toPromise();
      
          return { data: params.ids };
        },
}