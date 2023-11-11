pub struct Token(pub usize);

impl From<usize> for Token {
    fn from(val: uszie) -> Token {
        Token(val)
    }
}

impl From<Token> for usize {
    fn from(val: Token) -> usize {
        val.0
    }
}
