// cannot move out of `self.y` which is behind a mutable reference

struct S<'a> {
    x: &'a mut String,
    y: String,
}

impl<'a> S<'a> {
    fn getx(&mut self) -> &mut String {
        //&mut *self.x // Ok
        self.x // Ok
    }
    fn gety(&mut self) -> String {
        self.y // Err
    }
}

fn main() {

}

