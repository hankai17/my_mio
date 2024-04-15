//cannot borrow `node.job` as mutable, as it is behind a `&` reference
use std::fmt;

#[derive(Clone, Copy)]
struct Animal<'a> {
    name: &'a str
}

impl <'a>Animal<'a> {
    fn new(name: &'a str) ->  Self { 
        Self { 
            name 
        } 
    }    
}

struct Barn<'a> {
    name: &'a str,
    animals: Vec<Animal<'a>>
}

impl <'a> Barn<'a> {
    fn new(name: &'a str, animals: Vec<Animal<'a>>) ->  Self { 
        Self { 
            name, 
            animals 
        } 
    }    
}

impl <'a>fmt::Debug for Barn<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

struct Farmer<'a> {
    name: &'a str,
    barns: Vec<Barn<'a>>
}

impl <'a> Farmer<'a> {
    fn new(name: &'a str, barns: Vec<Barn<'a>>) -> Self { Self { name, barns } }
    
    fn add_new_barn(&mut self, barn: Barn<'a>) {
        self.barns.push(barn)
    }
    
    fn place_animal_in_barn(&mut self, animal: Animal, placement: &str) {
        for barn in &mut self.barns {
            // how do I do make this work?
            if barn.name == placement {
                barn.animals.push(animal);
            }
        }
    }

	/*
	fn place_animal_in_barn(&mut self, animal: Animal<'a>, placement: &str) {
        for barn in &mut self.barns {
            if barn.name == placement {
                barn.animals.push(animal);
            }
        }
    }
	*/
}

impl <'a>fmt::Debug for Farmer<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.barns)
    }
}

fn main() {
    let mut farmer = Farmer::new("Farm Master Joe",
        vec![
            Barn::new("Tegridy Farms", vec![
                Animal::new("cow"), 
                Animal::new("horse"), 
                Animal::new("pigeon")
            ]), 
            Barn::new("Barney", vec![
                Animal::new("Purple Dinosaur"), 
                Animal::new("Green Dinosaur")
            ]), 
            Barn::new("White Rabbits R' Us", vec![
                Animal::new("white rabbit"),
            ])
        ]
    );
    
    println!("{:#?}", farmer.name);
    farmer.add_new_barn(Barn::new("The Good Place", vec![
        Animal::new("Horse"),
        Animal::new("Wombat")
    ]));
    
    farmer.place_animal_in_barn(Animal::new("Turkey"), "The Good Place");
    println!("{:#?}", farmer);
}

