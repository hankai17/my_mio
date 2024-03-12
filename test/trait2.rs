trait OnOff {
	fn on(self)->Self;
	fn off(self)->Self;
}

struct BasicSwitch{
	on:bool,
}

impl BasicSwitch{
	fn on(mut self)->Self{
		self.on = true;
	}
	fn off(mut self)->Self{
		self.on=false;
	}
}

fn main() {

}
