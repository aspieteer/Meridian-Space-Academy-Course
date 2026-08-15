#[derive(Debug)]
pub struct Command {
    priority: u8,
    kind: CommandKind,
    issued_at: std::time::Instant,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CommandKind {
    SafeMode,
    AbortPass,
    Repoint { azimuth: EqF32, elevation: EqF32 },
    StatusRequest,
    Housekeeping,
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.issued_at == other.issued_at
    }
}
impl Eq for Command {}

impl Ord for Command {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compares priority first; if priority are equal, compares issued_at.
        // higher priority and less issued_at come first.
        self.priority
            .cmp(&other.priority)
            // Within the same priority, older commands go first.
            .then_with(|| other.issued_at.cmp(&self.issued_at))
    }
}
impl PartialOrd for Command {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Command {
    pub fn new(cmd_kind: CommandKind) -> Self {
        let priority: u8 = match cmd_kind {
            CommandKind::SafeMode => 255,
            CommandKind::AbortPass => 200,
            CommandKind::Repoint { .. } => 100,
            CommandKind::StatusRequest => 50,
            CommandKind::Housekeeping => 10,
        };

        Self {
            priority,
            kind: cmd_kind,
            issued_at: std::time::Instant::now(),
        }
    }

    pub fn kind_and_priority(&self) -> (&CommandKind, u8) {
        (&self.kind, self.priority)
    }
}

// ===== EqF32 =====

#[derive(Debug, Copy, Clone)]
pub struct EqF32(pub f32);

impl PartialEq for EqF32 {
    fn eq(&self, other: &Self) -> bool {
        // Treat NaN as equal to NaN to satisfy Eq reflexivity
        (self.0.is_nan() && other.0.is_nan()) || (self.0 == other.0)
    }
}
impl Eq for EqF32 {}

// ===== tests =====

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_command_order() {
        let cmd1 = Command::new(CommandKind::AbortPass);
        let cmd2 = Command::new(CommandKind::Housekeeping);
        let cmd3 = Command::new(CommandKind::SafeMode);
        assert!(cmd1 > cmd2 && cmd1 < cmd3 && cmd2 < cmd3);

        std::thread::sleep(Duration::from_nanos(500));

        let cmd4 = Command::new(CommandKind::SafeMode);
        let cmd5 = Command::new(CommandKind::Housekeeping);
        assert!(cmd1 > cmd5 && cmd1 < cmd4);
        assert!(cmd2 > cmd5 && cmd3 > cmd4);
    }
}
