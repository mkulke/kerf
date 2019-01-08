#[derive(Clone)]
pub enum Role {
    Follower(bool), // voted for yet
    Candidate(u8),
}
