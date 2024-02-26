from        to 	            函数
&str        String	        String::from(s) 或 s.to_string() 或 s.to_owned()
&str        &[u8]           s.as_bytes()
&str        Vec<u8>	        s.as_bytes().to_vec()
String	    &[u8]           s.as_bytes()
String	    &str            s.as_str() 或 &s
String	    Vec<u8>	        s.into_bytes()
&[u8]       &str            std::str::from_utf8(s).unwrap()
&[u8]       String          String::from_utf8(s).unwrap()
&[u8]       Vec<u8>	        s.to_vec()
Vec<u8>	    &str            std::str::from_utf8(&s).unwrap()
Vec<u8>	    String	        String::from_utf8(s).unwrap()
Vec<u8>	    &[u8]           &s 或 s.as_slice()
&[u8]       *const u8       s.as_ptr()
