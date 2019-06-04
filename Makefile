test:
	coverage erase
	python `which nosetests` -dsv --nologcapture --with-coverage --cover-package capgains tests/*.py

clean:
	find -regex '.*\.pyc' -exec rm {} \;
	find -regex '.*~' -exec rm {} \;
	rm -rf reg-settings.py
	rm -rf MANIFEST dist build *.egg-info
	rm -rf test.db

install:
	make clean
	make uninstall
	python setup.py install

uninstall:
	pip uninstall -y capgains

lint:
	pylint capgains/*.py

lint-tests:
	pylint tests/*.py

.PHONY:	test clean lint lint-tests install uninstall
