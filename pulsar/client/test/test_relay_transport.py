"""
Tests for the relay transport implementation.

Tests retry logic and message ID tracking functionality.
"""
from unittest.mock import Mock, patch
import pytest
import requests

from pulsar.client.transport.relay import RelayTransport, RelayTransportError


class TestRetryLogic:
    """Test retry logic with exponential backoff."""

    @patch('pulsar.client.transport.relay.time.sleep')
    def test_post_message_retries_on_connection_error(self, mock_sleep):
        """Test that post_message retries indefinitely on connection errors."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)

        # Mock the auth manager to return a token
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Mock session.post to fail twice with ConnectionError, then succeed
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'message_id': 'msg_123',
            'topic': 'test-topic',
            'timestamp': '2025-10-27T10:00:00Z'
        }

        transport.session.post = Mock(
            side_effect=[
                requests.ConnectionError("Connection refused"),
                requests.ConnectionError("Connection refused"),
                mock_response
            ]
        )

        result = transport.post_message('test-topic', {'data': 'test'})

        # Verify it succeeded after retries
        assert result['message_id'] == 'msg_123'
        assert transport.session.post.call_count == 3
        # Verify exponential backoff was used
        assert mock_sleep.call_count == 2
        # First delay should be 1.0, second should be 2.0
        assert mock_sleep.call_args_list[0][0][0] == 1.0
        assert mock_sleep.call_args_list[1][0][0] == 2.0

    @patch('pulsar.client.transport.relay.time.sleep')
    def test_post_message_retries_on_500_error(self, mock_sleep):
        """Test that post_message retries on 5xx server errors."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Mock responses: 500, 503, then 200
        mock_500 = Mock()
        mock_500.status_code = 500

        mock_503 = Mock()
        mock_503.status_code = 503

        mock_200 = Mock()
        mock_200.status_code = 200
        mock_200.json.return_value = {
            'message_id': 'msg_456',
            'topic': 'test-topic',
            'timestamp': '2025-10-27T10:00:00Z'
        }

        transport.session.post = Mock(side_effect=[mock_500, mock_503, mock_200])

        result = transport.post_message('test-topic', {'data': 'test'})

        assert result['message_id'] == 'msg_456'
        assert transport.session.post.call_count == 3
        assert mock_sleep.call_count == 2

    def test_post_message_does_not_retry_on_400_error(self):
        """Test that post_message does NOT retry on 4xx client errors."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Mock response with 400 error
        mock_400 = Mock()
        mock_400.status_code = 400

        # Create HTTPError with response attached
        error = requests.HTTPError("400 Bad Request")
        error.response = mock_400
        mock_400.raise_for_status.side_effect = error

        transport.session.post = Mock(return_value=mock_400)

        with pytest.raises(RelayTransportError):
            transport.post_message('test-topic', {'data': 'test'})

        # Should only be called once (no retries for 4xx)
        assert transport.session.post.call_count == 1

    @patch('pulsar.client.transport.relay.time.sleep')
    def test_post_message_retries_on_timeout(self, mock_sleep):
        """Test that post_message retries on timeout."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'message_id': 'msg_789',
            'topic': 'test-topic',
            'timestamp': '2025-10-27T10:00:00Z'
        }

        transport.session.post = Mock(
            side_effect=[
                requests.Timeout("Request timed out"),
                mock_response
            ]
        )

        result = transport.post_message('test-topic', {'data': 'test'})

        assert result['message_id'] == 'msg_789'
        assert transport.session.post.call_count == 2
        assert mock_sleep.call_count == 1

    @patch('pulsar.client.transport.relay.time.sleep')
    def test_retry_backoff_caps_at_max_delay(self, mock_sleep):
        """Test that exponential backoff caps at max_delay."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Create many connection errors to test max delay
        errors = [requests.ConnectionError("Connection refused")] * 10

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'message_id': 'msg_999',
            'topic': 'test-topic',
            'timestamp': '2025-10-27T10:00:00Z'
        }

        transport.session.post = Mock(side_effect=errors + [mock_response])

        result = transport.post_message('test-topic', {'data': 'test'})

        assert result['message_id'] == 'msg_999'
        assert mock_sleep.call_count == 10

        # Check that delay caps at 60 seconds
        delays = [call[0][0] for call in mock_sleep.call_args_list]
        # Expected: 1, 2, 4, 8, 16, 32, 60, 60, 60, 60
        assert delays[0] == 1.0
        assert delays[1] == 2.0
        assert delays[2] == 4.0
        assert delays[3] == 8.0
        assert delays[4] == 16.0
        assert delays[5] == 32.0
        # After this, should cap at 60
        assert all(d == 60.0 for d in delays[6:])


class TestMessageIDTracking:
    """Test message ID tracking functionality."""

    def test_long_poll_tracks_message_ids(self):
        """Test that long_poll tracks message IDs per topic."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Mock response with messages from different topics
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'messages': [
                {'topic': 'topic1', 'message_id': 'msg_001', 'payload': {'data': 'a'}},
                {'topic': 'topic2', 'message_id': 'msg_002', 'payload': {'data': 'b'}},
                {'topic': 'topic1', 'message_id': 'msg_003', 'payload': {'data': 'c'}},
            ],
            'has_more': False
        }

        transport.session.post = Mock(return_value=mock_response)

        messages = transport.long_poll(['topic1', 'topic2'])

        # Verify message IDs are tracked (last message ID per topic)
        assert transport.get_last_message_id('topic1') == 'msg_003'
        assert transport.get_last_message_id('topic2') == 'msg_002'
        assert len(messages) == 3

    @patch('pulsar.client.transport.relay.RelayTransport._persist_state_to_relay')
    def test_long_poll_uses_tracked_message_ids_in_since(self, mock_persist):
        """Test that long_poll includes tracked message IDs in the since parameter."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Set some tracked message IDs
        transport.set_last_message_id('topic1', 'msg_100')
        transport.set_last_message_id('topic2', 'msg_200')

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'messages': [],
            'has_more': False
        }

        transport.session.post = Mock(return_value=mock_response)

        # Call long_poll
        transport.long_poll(['topic1', 'topic2'])

        # Verify the 'since' parameter was included in the request
        call_args = transport.session.post.call_args
        request_json = call_args[1]['json']

        assert 'since' in request_json
        assert request_json['since']['topic1'] == 'msg_100'
        assert request_json['since']['topic2'] == 'msg_200'

    @patch('pulsar.client.transport.relay.RelayTransport._persist_state_to_relay')
    def test_long_poll_only_includes_since_for_requested_topics(self, mock_persist):
        """Test that since only includes tracked IDs for topics in the request."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Set tracked message IDs for multiple topics
        transport.set_last_message_id('topic1', 'msg_100')
        transport.set_last_message_id('topic2', 'msg_200')
        transport.set_last_message_id('topic3', 'msg_300')

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'messages': [], 'has_more': False}

        transport.session.post = Mock(return_value=mock_response)

        # Only poll for topic1 and topic2
        transport.long_poll(['topic1', 'topic2'])

        call_args = transport.session.post.call_args
        request_json = call_args[1]['json']

        # Should only include topic1 and topic2 in since
        assert 'since' in request_json
        assert 'topic1' in request_json['since']
        assert 'topic2' in request_json['since']
        assert 'topic3' not in request_json['since']


class TestStatePersistence:
    """Test state persistence functionality."""

    def test_load_state_from_relay_on_init(self):
        """Test that RelayTransport loads persisted state on initialization."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Mock the session to return a state message
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'messages': [
                {
                    'topic': 'consumer_state',
                    'message_id': 'state_001',
                    'payload': {
                        'manager_name': '_default_',
                        'timestamp': 1234567890.0,
                        'tracked_since': {
                            'job_setup': 'msg_100',
                            'job_status_request': 'msg_200',
                            'job_kill': 'msg_300'
                        }
                    }
                }
            ]
        }
        transport.session.post = Mock(return_value=mock_response)

        # Manually call load state
        transport._load_state_from_relay()

        # Verify state was loaded
        assert transport.get_last_message_id('job_setup') == 'msg_100'
        assert transport.get_last_message_id('job_status_request') == 'msg_200'
        assert transport.get_last_message_id('job_kill') == 'msg_300'

    def test_load_state_with_prefix_and_manager(self):
        """Test state loading with topic prefix and custom manager name."""
        transport = RelayTransport(
            'http://localhost:8000',
            'user',
            'pass',
            topic_prefix='galaxy',
            manager_name='cluster1',
            load_state=False
        )

        # Verify the state topic name was built correctly
        assert transport._state_topic == 'galaxy_consumer_state_cluster1'

    def test_init_handles_no_persisted_state(self):
        """Test that initialization handles the case when no state exists."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Mock long_poll to return empty messages
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'messages': []}
        transport.session.post = Mock(return_value=mock_response)

        transport._load_state_from_relay()

        # Should have empty tracking
        assert len(transport.get_all_tracked_message_ids()) == 0

    def test_init_handles_state_load_failure(self):
        """Test that initialization continues if state loading fails."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)
        transport.auth_manager.get_token = Mock(return_value='test-token')

        # Mock session to raise an exception
        transport.session.post = Mock(side_effect=Exception("Network error"))

        # Should not raise, just log warning
        transport._load_state_from_relay()

        # Should have empty tracking
        assert len(transport.get_all_tracked_message_ids()) == 0

    @patch('pulsar.client.transport.relay.RelayTransport.post_message')
    def test_persist_state_after_message_tracking(self, mock_post):
        """Test that state is persisted after tracking a message."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)

        # Set a message ID (this should trigger persistence)
        transport.set_last_message_id('topic1', 'msg_123')

        # Verify post_message was called to persist state
        mock_post.assert_called_once()
        call_args = mock_post.call_args

        # Check the topic is the state topic
        assert call_args[1]['topic'] == 'consumer_state'

        # Check the payload contains the tracked state
        payload = call_args[1]['payload']
        assert payload['manager_name'] == '_default_'
        assert 'timestamp' in payload
        assert payload['tracked_since']['topic1'] == 'msg_123'

    @patch('pulsar.client.transport.relay.RelayTransport.post_message')
    def test_persist_state_on_close(self, mock_post):
        """Test that state is persisted when transport is closed."""
        transport = RelayTransport('http://localhost:8000', 'user', 'pass', load_state=False)

        # Track some messages
        transport._last_message_ids['topic1'] = 'msg_100'
        transport._last_message_ids['topic2'] = 'msg_200'

        # Reset mock to ignore persistence calls during set_last_message_id
        mock_post.reset_mock()

        # Close the transport
        transport.close()

        # Verify post_message was called to persist final state
        mock_post.assert_called_once()
        call_args = mock_post.call_args

        payload = call_args[1]['payload']
        assert payload['tracked_since']['topic1'] == 'msg_100'
        assert payload['tracked_since']['topic2'] == 'msg_200'

    def test_make_topic_name_with_prefix(self):
        """Test topic name generation with prefix."""
        transport = RelayTransport(
            'http://localhost:8000',
            'user',
            'pass',
            topic_prefix='galaxy',
            load_state=False
        )

        topic_name = transport._make_topic_name('job_setup')
        assert topic_name == 'galaxy_job_setup'

    def test_make_topic_name_with_manager(self):
        """Test topic name generation with custom manager name."""
        transport = RelayTransport(
            'http://localhost:8000',
            'user',
            'pass',
            manager_name='cluster1',
            load_state=False
        )

        topic_name = transport._make_topic_name('job_setup')
        assert topic_name == 'job_setup_cluster1'

    def test_make_topic_name_with_prefix_and_manager(self):
        """Test topic name generation with both prefix and manager."""
        transport = RelayTransport(
            'http://localhost:8000',
            'user',
            'pass',
            topic_prefix='galaxy',
            manager_name='cluster1',
            load_state=False
        )

        topic_name = transport._make_topic_name('job_setup')
        assert topic_name == 'galaxy_job_setup_cluster1'

    def test_make_topic_name_default_manager(self):
        """Test topic name generation with default manager (no suffix)."""
        transport = RelayTransport(
            'http://localhost:8000',
            'user',
            'pass',
            topic_prefix='galaxy',
            manager_name='_default_',
            load_state=False
        )

        topic_name = transport._make_topic_name('job_setup')
        # Default manager should not add suffix
        assert topic_name == 'galaxy_job_setup'
