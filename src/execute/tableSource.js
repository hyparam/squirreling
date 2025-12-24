import { tableNotFoundError } from '../executionErrors.js'
import { generatorSource } from '../backend/dataSource.js'

/**
 * @import { AsyncDataSource, CTEDefinition, UserDefinedFunction, WithClause } from '../types.js'
 */

/**
 * Gets CTEs defined before the target CTE (excluding the target itself).
 * Enforces SQL scoping rules: each CTE can only reference CTEs defined before it.
 *
 * @param {CTEDefinition[]} allCtes - all CTE definitions in order
 * @param {string} targetCteName - the CTE name (case-insensitive)
 * @returns {WithClause} CTEs available to the target
 */
export function getCtesDefinedBefore(allCtes, targetCteName) {
  const available = []
  for (const cte of allCtes) {
    if (cte.name.toLowerCase() === targetCteName) break
    available.push(cte)
  }
  return { ctes: available }
}

/**
 * Resolves a table name to an AsyncDataSource, checking CTEs first
 *
 * @param {string} tableName - the table name to resolve
 * @param {Record<string, AsyncDataSource>} tables - regular tables
 * @param {import('../types.js').WithClause} [withClause] - WITH clause containing CTE definitions
 * @param {Function} [executeSelectFn] - function to execute SELECT for CTEs
 * @param {Record<string, UserDefinedFunction>} [functions]
 * @param {AbortSignal} [signal]
 * @returns {AsyncDataSource}
 */
export function resolveTableSource(tableName, tables, withClause, executeSelectFn, functions, signal) {
  // Check CTEs first (case-insensitive) - only build map when CTE is actually found
  if (withClause && executeSelectFn) {
    const lowerName = tableName.toLowerCase()
    const cte = withClause.ctes.find(c => c.name.toLowerCase() === lowerName)

    if (cte) {
      // CTE reference: wrap in generatorSource, re-execute each time (streaming)
      // Pass only CTEs defined before this one to prevent self-reference
      const availableCtes = getCtesDefinedBefore(withClause.ctes, lowerName)

      return generatorSource(executeSelectFn({
        select: cte.query,
        tables,
        withClause: availableCtes,
        functions,
        signal,
      }))
    }
  }

  // Regular table lookup
  const tableSource = tables[tableName]
  if (tableSource === undefined) {
    throw tableNotFoundError({ tableName })
  }
  return tableSource
}
