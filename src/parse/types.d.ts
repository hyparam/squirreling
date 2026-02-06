import { UserDefinedFunction } from '../types.js'

export interface ParserState {
  tokens: Token[]
  pos: number
  lastPos?: number
  functions?: Record<string, UserDefinedFunction>
}

// Tokenizer types
export type TokenType =
  | 'keyword'
  | 'identifier'
  | 'number'
  | 'string'
  | 'operator'
  | 'comma'
  | 'dot'
  | 'paren'
  | 'semicolon'
  | 'eof'

export interface Token {
  type: TokenType
  value: string
  positionStart: number
  positionEnd: number
  numericValue?: number | bigint
  originalValue?: string
}
