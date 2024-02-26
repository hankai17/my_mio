extern crate bytes;
use std::{io, mem, fmt};
use bytes::{Buf, BufMut, Bytes, BytesMut};

fn bytes_into_vec() {
    // Test kind == KIND_VEC
    let content = b"helloworld";

    let mut bytes = BytesMut::new();
    bytes.put_slice(content);

    let vec: Vec<u8> = bytes.into();
    assert_eq!(&vec, content);

    // Test kind == KIND_ARC, shared.is_unique() == True
    let mut bytes = BytesMut::new();
    bytes.put_slice(b"abcdewe23");
    bytes.put_slice(content);

    // Overwrite the bytes to make sure only one reference to the underlying
    // Vec exists.
    bytes = bytes.split_off(9);

    let vec: Vec<u8> = bytes.into();
    assert_eq!(&vec, content);

    // Test kind == KIND_ARC, shared.is_unique() == False
    let prefix = b"abcdewe23";

    let mut bytes = BytesMut::new();
    bytes.put_slice(prefix);
    bytes.put_slice(content);

    let vec: Vec<u8> = bytes.split_off(prefix.len()).into();
    assert_eq!(&vec, content);

    let vec: Vec<u8> = bytes.into();
    assert_eq!(&vec, prefix);
}

fn test_bytes_into_vec() {
    // Test STATIC_VTABLE.to_vec
    let bs = b"1b23exfcz3r";
    let vec: Vec<u8> = Bytes::from_static(bs).into();
    assert_eq!(&*vec, bs);

    // Test bytes_mut.SHARED_VTABLE.to_vec impl
    eprintln!("1");
    let mut bytes_mut: BytesMut = bs[..].into();

    // Set kind to KIND_ARC so that after freeze, Bytes will use bytes_mut.SHARED_VTABLE
    eprintln!("2");
    drop(bytes_mut.split_off(bs.len()));

    eprintln!("3");
    let b1 = bytes_mut.freeze();
    eprintln!("4");
    let b2 = b1.clone();

    eprintln!("{:#?}", (&*b1).as_ptr());

    // shared.is_unique() = False
    eprintln!("5");
    assert_eq!(&*Vec::from(b2), bs);

    // shared.is_unique() = True
    eprintln!("6");
    assert_eq!(&*Vec::from(b1), bs);

    // Test bytes_mut.SHARED_VTABLE.to_vec impl where offset != 0
    let mut bytes_mut1: BytesMut = bs[..].into();
    let bytes_mut2 = bytes_mut1.split_off(9);

    let b1 = bytes_mut1.freeze();
    let b2 = bytes_mut2.freeze();

    assert_eq!(Vec::from(b2), bs[9..]);
    assert_eq!(Vec::from(b1), bs[..9]);
}

fn test_bytes_into_vec_promotable_even() {
    let vec = vec![33u8; 1024];

    // Test cases where kind == KIND_VEC
    //let c1 = vec.clone();
    let b1 = Bytes::from(vec.clone());
    assert_eq!(Vec::from(b1), vec);

    // Test cases where kind == KIND_ARC, ref_cnt == 1
    //let c2 = vec.clone();
    let b1 = Bytes::from(vec.clone());
    drop(b1.clone());
    assert_eq!(Vec::from(b1), vec);

    // Test cases where kind == KIND_ARC, ref_cnt == 2
    let b1 = Bytes::from(vec.clone());
    let b2 = b1.clone();
    assert_eq!(Vec::from(b1), vec);

    // Test cases where vtable = SHARED_VTABLE, kind == KIND_ARC, ref_cnt == 1
    assert_eq!(Vec::from(b2), vec);

    // Test cases where offset != 0
    let mut b1 = Bytes::from(vec.clone());
    let b2 = b1.split_off(20);

    assert_eq!(Vec::from(b2), vec[20..]);
    assert_eq!(Vec::from(b1), vec[..20]);
}

/*
#[test]
fn test_bytes_vec_conversion() {
    let mut vec = Vec::with_capacity(10);
    vec.extend(b"abcdefg");
    let b = Bytes::from(vec);
    let v = Vec::from(b);
    assert_eq!(v.len(), 7);
    assert_eq!(v.capacity(), 10);

    let mut b = Bytes::from(v);
    b.advance(1);
    let v = Vec::from(b);
    assert_eq!(v.len(), 6);
    assert_eq!(v.capacity(), 10);
    assert_eq!(v.as_slice(), b"bcdefg");
}

#[test]
fn test_bytes_mut_conversion() {
    let mut b1 = BytesMut::with_capacity(10);
    b1.extend(b"abcdefg");
    let b2 = Bytes::from(b1);
    let v = Vec::from(b2);
    assert_eq!(v.len(), 7);
    assert_eq!(v.capacity(), 10);

    let mut b = Bytes::from(v);
    b.advance(1);
    let v = Vec::from(b);
    assert_eq!(v.len(), 6);
    assert_eq!(v.capacity(), 10);
    assert_eq!(v.as_slice(), b"bcdefg");
}

#[test]
fn test_bytes_capacity_len() {
    for cap in 0..100 {
        for len in 0..=cap {
            let mut v = Vec::with_capacity(cap);
            v.resize(len, 0);
            let _ = Bytes::from(v);
        }
    }
}

#[test]
fn static_is_unique() {
    let b = Bytes::from_static(LONG);
    assert!(!b.is_unique());
}

#[test]
fn vec_is_unique() {
    let v: Vec<u8> = LONG.to_vec();
    let b = Bytes::from(v);
    assert!(b.is_unique());
}

#[test]
fn arc_is_unique() {
    let v: Vec<u8> = LONG.to_vec();
    let b = Bytes::from(v);
    let c = b.clone();
    assert!(!b.is_unique());
    drop(c);
    assert!(b.is_unique());
}

#[test]
fn shared_is_unique() {
    let v: Vec<u8> = LONG.to_vec();
    let b = Bytes::from(v);
    let c = b.clone();
    assert!(!c.is_unique());
    drop(b);
    assert!(c.is_unique());
}
*/

fn main() {
    //bytes_into_vec();
    test_bytes_into_vec_promotable_even();
}

