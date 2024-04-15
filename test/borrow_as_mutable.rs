// cannot borrow `node.job` as mutable, as it is behind a `&` reference

struct Row {
	string: String,
}

impl Row {
	fn insert(&mut self, at: u16, c: char) {
		let at = at as usize;
		if at >= self.string.len() {
			self.string.push(c);
		} else {
			self.string.insert(at, c)
		}
	}
}

struct File {
	rows: Vec<Row>,
	filename: Option<String>
}

impl File {
	/*
	fn row(&self, index: u16) -> &Row{
		&self.rows[index as usize]
	}
	*/
	fn row(&self, index: usize) -> &Row {
	    &self.rows[index]
	}

	fn row_mut(&mut self, index: usize) -> &mut Row {
	    &mut self.rows[index]
	}
	/*
	fn row(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }
    fn row_mut(&mut self, index: usize) -> Option<&mut Row> {
        self.rows.get_mut(index)
    }
    */
}

struct EditorState {
	file: File,
}

//In the actual functon, I am capturing the keypress,
//get the correct row from the state and pass it and the pressed
// char to row.insert
fn update_row(mut state: &mut EditorState)  {
	//let row = &state.file.row(0);
	let row = &mut state.file.row_mut(0);
	row.insert(0, 'a');     // cannot borrow `**row` as mutable, as it is behind a `&` reference
}

fn main() {

}

