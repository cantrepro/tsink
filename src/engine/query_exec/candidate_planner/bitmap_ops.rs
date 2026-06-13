use roaring::RoaringTreemap;

pub(super) fn intersect_candidates(
    candidates: &mut RoaringTreemap,
    filter: Option<&RoaringTreemap>,
) {
    match filter {
        Some(filter) => *candidates &= filter,
        None => candidates.clear(),
    }
}

pub(super) fn subtract_candidates(
    candidates: &mut RoaringTreemap,
    filter: Option<&RoaringTreemap>,
) {
    if let Some(filter) = filter {
        *candidates -= filter;
    }
}

pub(super) fn bitmap_cardinality(bitmap: &RoaringTreemap) -> usize {
    usize::try_from(bitmap.len()).unwrap_or(usize::MAX)
}
