struct HasDrop1;
impl Drop for HasDrop1 {
    fn drop(&mut self) {
        println!("Dropping HasDrop1!");
    }
}

struct HasDrop2;
impl Drop for HasDrop2 {
    fn drop(&mut self) {
        println!("Dropping HasDrop2!");
    }
}

struct HasTwoDrops {
    one: HasDrop1,
    two: HasDrop2,
}

impl Drop for HasTwoDrops {
    fn drop(&mut self) {
        println!("Dropping HasTwoDrops!");
    }
}

struct Foo;
impl Drop for Foo {
    fn drop(&mut self) {
        println!("Dropping Foo!")
    }
}

fn main1() {
    let _x = HasTwoDrops {
        two: HasDrop2,
        one: HasDrop1,
    };
    let _foo = Foo;
    println!("Running!");
}

/*
Running!
Dropping Foo!
Dropping HasTwoDrops!
Dropping HasDrop1!
Dropping HasDrop2!
*/

#[derive(Debug)]
struct Foo1;

impl Drop for Foo1 {
    fn drop(&mut self) {
        println!("Dropping Foo1!")
    }
}

fn main() {
    let foo = Foo1;
    //foo.drop();
    //println!("Running!:{:?}", foo);

    drop(foo);
    println!("Running!:{:?}", foo);
}


