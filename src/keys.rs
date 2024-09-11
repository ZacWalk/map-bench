use std::hash::{Hash, Hasher};

#[derive(Clone, Default)]
pub(crate) struct StringKey(String);

impl From<u64> for StringKey {
    fn from(num: u64) -> Self {
        // Your conversion logic here
        StringKey(num.to_string())
    }
}

impl PartialEq for StringKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for StringKey {}

impl Hash for StringKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialOrd for StringKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0) // Compare the underlying strings
    }
}

impl Ord for StringKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0) // Compare the underlying strings
    }

    fn max(self, other: Self) -> Self
    where
        Self: Sized,
    {
        std::cmp::max_by(self, other, Ord::cmp)
    }

    fn min(self, other: Self) -> Self
    where
        Self: Sized,
    {
        std::cmp::min_by(self, other, Ord::cmp)
    }

    fn clamp(self, min: Self, max: Self) -> Self
    where
        Self: Sized,
    {
        assert!(min <= max);
        if self < min {
            min
        } else if self > max {
            max
        } else {
            self
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct StrKey<'a>(&'a str); 

impl<'a> From<u64> for StrKey<'a> {
    fn from(num: u64) -> Self {
        // Convert u64 to string and leak it to get a static str
        let s: &'static str = Box::leak(format!("{}", num).into_boxed_str());
        StrKey(s)
    }
}

impl<'a> Default for StrKey<'a> {
    fn default() -> Self {
        // You need a 'static str for the default value
        static DEFAULT_STR: &str = "";
        StrKey(DEFAULT_STR)
    }
}
