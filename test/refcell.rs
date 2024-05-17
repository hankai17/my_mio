use std::cell::RefCell;

struct Bank {
    balance: RefCell<i32>, 								// 使用RefCell存储余额，因为余额是内部可变的
}

impl Bank {
    fn new() -> Bank {
        Bank { balance: RefCell::new(0) }
    }

    fn deposit(&self, amount: i32) { 					// 存款
        let mut balance = self.balance.borrow_mut(); 	// 获取内部可变引用
        *balance += amount;
    }

    fn withdraw(&self, amount: i32) -> bool { 			// 取款
        let mut balance = self.balance.borrow_mut();
        if *balance >= amount {
            *balance -= amount;
            true
        } else {
            false
        }
    }
}

fn main() {
    let bank = Bank::new();
    bank.deposit(100);
    assert!(bank.withdraw(50));
    assert_eq!(*bank.balance.borrow(), 50);
}

// https://course.rs/advance/smart-pointer/cell-refcell.html
