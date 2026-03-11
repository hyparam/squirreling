---
name: add-sql-function
description: |
  How to implement a new SQL feature in squirreling.
---

# Adding a New SQL Function to Squirreling

## Process Overview

1. **Identify function category** - Determine which category the function belongs to
2. **Write a failing test** - Always start with a test that demonstrates the function is not yet implemented
3. **Register the function** - Add to type guard and `FUNCTION_SIGNATURES` in `src/validation/functions.js`
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

In `src/validation/functions.js`, add the function name to the appropriate type guard array (`isMathFunc`, `isStringFunc`, `isAggregateFunc`, or `isRegexpFunc`).

In `src/validation/functions.js`, add to `FUNCTION_SIGNATURES`:

```javascript
NEW_FUNCTION: { min: 1, max: 1, signature: 'value' },
// or: { min: 2, max: 2, signature: 'a, b' },
// or: { min: 1, max: 3, signature: 'required[, opt1[, opt2]]' },
// or: { min: 1, signature: 'value1[, ...]' },
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
