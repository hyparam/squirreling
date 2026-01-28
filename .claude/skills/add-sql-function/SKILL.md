---
name: add-sql-function
description: |
  How to implement a new SQL feature in squirreling.
---

# Adding a New SQL Function to Squirreling

## Process Overview

1. **Identify function category** - Determine which category the function belongs to
2. **Write a failing test** - Always start with a test that demonstrates the function is not yet implemented
3. **Add to type guard** - Register the function in the appropriate type guard in `src/validation.js`
4. **Add argument count** - Specify min/max args in `FUNCTION_ARG_COUNTS` in `src/validation.js`
5. **Implement the function** - Add the execution logic in the appropriate file
6. **Run all checks** - Ensure lint, tests, and TypeScript all pass
7. **Update README** - Add the function to the appropriate list (if it's a new built-in)
8. **Create a commit** - Commit the changes with a descriptive message

---

## Step 1: Write a Failing Test

**REQUIRED for all functions.**

Create tests in the appropriate test file. Include tests for:
- Basic functionality
- Null handling (SQL functions typically return null if any input is null)
- Wrong argument count (should throw)

**Run the test to confirm it fails:**
```bash
npx vitest test/execute/execute.math.test.js --run
```

---

## Step 2: Update Validation

**REQUIRED for:** Math, String, Aggregate, Regex functions.
**SKIP for:** Date/Time, JSON, Conditional functions (these use inline checks).

In `src/validation.js`, add the function name to the appropriate type guard array (`isMathFunc`, `isStringFunc`, `isAggregateFunc`, or `isRegexpFunc`).

In `src/validation.js`, add to `FUNCTION_ARG_COUNTS`:

```javascript
NEW_FUNCTION: { min: 1, max: 1 },  // exactly 1 arg
// or: { min: 2, max: 2 },         // exactly 2 args
// or: { min: 1, max: 3 },         // 1 to 3 args
// or: { min: 1 },                 // at least 1 arg (no max)
```

---

## Step 3: Implement the Function

**REQUIRED for all functions.**

Add implementation to the file determined in Step 1. Follow the pattern of existing functions in that file. Key points:
- Handle null inputs by returning null
- For math: convert to Number
- For strings: convert to String
- For aggregates: iterate over `filteredRows` inside the `isAggregateFunc` block

---

## Step 4: Run All Checks

**REQUIRED for all functions.**

All three must pass:

```bash
npm test              # All tests must pass
npm run lint          # No linting errors
npx tsc               # TypeScript must pass
```

---

## Step 5: Update README

**REQUIRED for:** New built-in functions that users should know about.
**SKIP for:** Internal helper functions or variations of existing functions.

Add the function to the appropriate list in the "Functions" section of `README.md`.
