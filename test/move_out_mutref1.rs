struct Foo<'a> {
    parent: Option<&'a mut Foo<'a>>,
    value: i32,
}

impl<'a> Foo<'a> {
    fn bar(&mut self) {
        //if let Some(&mut parent) = self.parent { // failed
        //if let Some(ref mut parent) = self.parent { // ok
        if let Some(parent) = self.parent.as_mut() {
            parent.bar();
        } else {
            self.value = 1;
        }
    }
}

fn main() {

}

