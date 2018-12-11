"""Test Alias Action"""
# pylint: disable=C0103,C0111
from unittest import TestCase
from mock import Mock, patch
import elasticsearch
from curator_api.actions import Alias
from curator_api.exceptions import ActionError, FailedExecution, NoIndices
# Get test variables and constants from a single source
from . import testvars as testvars

ALIAS = 'testalias'
IDX1 = 'my_index'
IDX2 = 'dummy'

class TestActionAlias(TestCase):
    """Test Alias action class"""
    def test_raise_exception_on_empty_index_list(self):
        client = Mock()
        _ = Alias(client, ALIAS)
        self.assertRaises(NoIndices, _.add, [])
        self.assertRaises(NoIndices, _.remove, [])
        self.assertRaises(ActionError, _.body)
    def test_warn_on_empty_index_list(self):
        client = Mock()
        _ = Alias(client, ALIAS)
        self.assertIsNone(_.add([], warn_if_no_indices=True))
        self.assertIsNone(_.remove([], warn_if_no_indices=True))
        self.assertRaises(NoIndices, _.body)
    def test_do_action_raises_exception(self):
        client = Mock()
        # client.indices.get_settings.return_value = testvars.settings_one
        # client.cluster.state.return_value = testvars.clu_state_one
        # client.indices.stats.return_value = testvars.stats_one
        client.indices.update_aliases.return_value = testvars.alias_success
        client.indices.update_aliases.side_effect = testvars.four_oh_one
        _ = Alias(client, name=ALIAS)
        _.add([IDX1, IDX2])
        self.assertRaises(FailedExecution, _.do_action)

    # def test_add_raises_on_invalid_parameter(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_one
    #     client.cluster.state.return_value = testvars.clu_state_one
    #     client.indices.stats.return_value = testvars.stats_one
    #     _ = curator_api.IndexList(client)
    #     ao = curator_api.Alias(name='alias')
    #     self.assertRaises(TypeError, ao.add, [])
    # def test_add_single(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_one
    #     client.cluster.state.return_value = testvars.clu_state_one
    #     client.indices.stats.return_value = testvars.stats_one
    #     _ = curator_api.IndexList(client)
    #     ao = curator_api.Alias(name='alias')
    #     ao.add(_)
    #     self.assertEqual(testvars.alias_one_add, ao.actions)
    # def test_add_single_with_extra_settings(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_one
    #     client.cluster.state.return_value = testvars.clu_state_one
    #     client.indices.stats.return_value = testvars.stats_one
    #     _ = curator_api.IndexList(client)
    #     esd = {
    #         'filter' : { 'term' : { 'user' : 'kimchy' } }
    #     }
    #     ao = curator_api.Alias(name='alias', extra_settings=esd)
    #     ao.add(_)
    #     self.assertEqual(testvars.alias_one_add_with_extras, ao.actions)
    # def test_remove_single(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_one
    #     client.cluster.state.return_value = testvars.clu_state_one
    #     client.indices.stats.return_value = testvars.stats_one
    #     client.indices.get_alias.return_value = testvars.settings_1_get_aliases
    #     _ = curator_api.IndexList(client)
    #     ao = curator_api.Alias(name='my_alias')
    #     ao.remove(_)
    #     self.assertEqual(testvars.alias_one_rm, ao.actions)
    # def test_add_multiple(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_two
    #     client.cluster.state.return_value = testvars.clu_state_two
    #     client.indices.stats.return_value = testvars.stats_two
    #     _ = curator_api.IndexList(client)
    #     ao = curator_api.Alias(name='alias')
    #     ao.add(_)
    #     cmp = sorted(ao.actions, key=lambda k: k['add']['index'])
    #     self.assertEqual(testvars.alias_two_add, cmp)
    # def test_remove_multiple(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_two
    #     client.cluster.state.return_value = testvars.clu_state_two
    #     client.indices.stats.return_value = testvars.stats_two
    #     client.indices.get_alias.return_value = testvars.settings_2_get_aliases
    #     _ = curator_api.IndexList(client)
    #     ao = curator_api.Alias(name='my_alias')
    #     ao.remove(_)
    #     cmp = sorted(ao.actions, key=lambda k: k['remove']['index'])
    #     self.assertEqual(testvars.alias_two_rm, cmp)
    # def test_raise_action_error_on_empty_body(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_one
    #     client.cluster.state.return_value = testvars.clu_state_one
    #     client.indices.stats.return_value = testvars.stats_one
    #     _ = curator_api.IndexList(client)
    #     ao = curator_api.Alias(name='alias')
    #     self.assertRaises(curator_api.ActionError, ao.body)
    # def test_raise_no_indices_on_empty_body_when_warn_if_no_indices(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_one
    #     client.cluster.state.return_value = testvars.clu_state_one
    #     client.indices.stats.return_value = testvars.stats_one
    #     _ = curator_api.IndexList(client)
    #     # empty it, so there can be no body
    #     _.indices = []
    #     ao = curator_api.Alias(name='alias')
    #     ao.add(_, warn_if_no_indices=True)
    #     self.assertRaises(curator_api.NoIndices, ao.body)
    # def test_do_dry_run(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_one
    #     client.cluster.state.return_value = testvars.clu_state_one
    #     client.indices.stats.return_value = testvars.stats_one
    #     client.indices.update_aliases.return_value = testvars.alias_success
    #     _ = curator_api.IndexList(client)
    #     ao = curator_api.Alias(name='alias')
    #     ao.add(_)
    #     self.assertIsNone(ao.do_dry_run())
    # def test_do_action(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_one
    #     client.cluster.state.return_value = testvars.clu_state_one
    #     client.indices.stats.return_value = testvars.stats_one
    #     client.indices.update_aliases.return_value = testvars.alias_success
    #     _ = curator_api.IndexList(client)
    #     ao = curator_api.Alias(name='alias')
    #     ao.add(_)
    #     self.assertIsNone(ao.do_action())
    # def test_do_action_raises_exception(self):
    #     client = Mock()
    #     client.info.return_value = {'version': {'number': '5.0.0'} }
    #     client.indices.get_settings.return_value = testvars.settings_one
    #     client.cluster.state.return_value = testvars.clu_state_one
    #     client.indices.stats.return_value = testvars.stats_one
    #     client.indices.update_aliases.return_value = testvars.alias_success
    #     client.indices.update_aliases.side_effect = testvars.four_oh_one
    #     _ = curator_api.IndexList(client)
    #     ao = curator_api.Alias(name='alias')
    #     ao.add(_)
    #     self.assertRaises(curator_api.FailedExecution, ao.do_action)
