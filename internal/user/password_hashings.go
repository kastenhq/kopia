package user

// PasswordHashingAlgorithms returns the supported algorithms for user password hashing.
func PasswordHashingAlgorithms() []string {
	return []string{scryptHashAlgorithm, pbkdf2HashAlgorithm}
}
