use std::collections::HashMap;

fn main() {
	let mut map = HashMap::new();
	map.insert(1, "a");

	if let Some(x) = map.get_mut(&1) {
		*x = "b";
	}

	let v1 = map.get_mut(&1).unwrap();
	let v2 = map.get_mut(&1).unwrap();

    *v1 = "c";
    println!("v2 {}", *v2);

	//assert_eq!(map[&1], "b");
}

