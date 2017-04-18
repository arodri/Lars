from lars.mapper import Mapper
from collections import namedtuple

class Conditional(Mapper):

	_Comparator = namedtuple('Comparator',['compareLess','compareGreater','compareEqual'])
	_Rule = namedtuple('Rule',['field','comparator','threshold','add_if_true','else_add'])
	_NOT_PROVIDED = '-'

	def makeComparator(self, comparisonString):
		return self._Comparator('<' in comparisonString,'>' in comparisonString,'=' in comparisonString)

	def compare(self, testValue, comparator, threshold):
		if comparator.compareLess and testValue < threshold:
			return True
		elif comparator.compareGreater and testValue > threshold:
			return True
		elif comparator.compareEqual and testValue == threshold:
			return True
		else:
			return False

	def loadConfigJSON(self,config):
		self.fieldToScale = config['fieldToScale']
		self.scaledField = config.get('scaledField',self.fieldToScale)
		self.rules = []
		rules = config['rules']
		for r in rules:
			self.rules.append(self._Rule(r['field'],self.makeComparator(r['comparator']),r['threshold'],r.get('add_if_true',self._NOT_PROVIDED),r.get('else_add',self._NOT_PROVIDED)))
		self.max_add_if_true = config.get('max_add_if_true',self._NOT_PROVIDED)
		self.max_else_add = config.get('max_else_add',self._NOT_PROVIDED)
		self.floor = config.get('floor',self._NOT_PROVIDED)
		self.ceiling = config.get('ceiling',self._NOT_PROVIDED)
		self.output_type = config.get('output_type',self._NOT_PROVIDED)

	def process(self, record):
		result = record[self.fieldToScale]
		any_true = False
		total_add_if_true = 0
		total_else_add = 0

		for rule in self.rules:
			if self.compare(record[rule.field],rule.comparator,float(rule.threshold)):
				any_true = True
				if rule.add_if_true != self._NOT_PROVIDED:
					total_add_if_true += float(rule.add_if_true)
			else:
				if rule.else_add != self._NOT_PROVIDED:
					total_else_add += float(rule.else_add)

		if self.max_add_if_true != self._NOT_PROVIDED:
			total_add_if_true = self.max_add_if_true if abs(total_add_if_true) > abs(self.max_add_if_true) else total_add_if_true
		if self.max_else_add != self._NOT_PROVIDED:
			total_else_add = self.max_else_add if abs(total_add_if_true) > abs(self.max_else_add) else total_else_add

		result += total_add_if_true + total_else_add

		if any_true:
			if self.floor != self._NOT_PROVIDED:
				result = self.floor if result < self.floor else result
			if self.ceiling != self._NOT_PROVIDED:
				result = self.ceiling if result > self.ceiling else result

		if self.output_type == 'int':
			result = int(result)

		record[self.scaledField] = result
		return record

