package lib

// Close will cleanly shutdown the channel and connection.
func (s *Session) Close() error {
	if !s.isReady {
		return errAlreadyClosed
	}
	if err := s.channel.Close(); err != nil {
		return err
	}
	if err := s.connection.Close(); err != nil {
		return err
	}
	close(s.done)
	s.isReady = false
	return nil
}
