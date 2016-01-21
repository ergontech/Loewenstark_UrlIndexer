<?php
/**
  * Loewenstark_UrlIndexer
  *
  * @category  Loewenstark
  * @package   Loewenstark_UrlIndexer
  * @author    Mathis Klooss <m.klooss@loewenstark.com>
  * @copyright 2013 Loewenstark Web-Solution GmbH (http://www.mage-profis.de/). All rights served.
  * @license   https://github.com/mklooss/Loewenstark_UrlIndexer/blob/master/README.md
  */
class Loewenstark_UrlIndexer_Model_Resource_Url
extends Mage_Catalog_Model_Resource_Url
{
    
    /**
     * Limit products for select
     *
     * @var int
     */
    protected $_productLimit                = 250;

    /**
     * Retrieve categories objects
     * Either $categoryIds or $path (with ending slash) must be specified
     *
     * @param int|array $categoryIds
     * @param int $storeId
     * @param string $path
     * @return array
     */
    protected function _getCategories($categoryIds, $storeId = null, $path = null)
    {
        if($this->_helper()->HideDisabledCategories($storeId))
        {
            $categories = parent::_getCategories($categoryIds, $storeId, $path);
            if($categories)
            {
                $category = end($categories);
                $attributes = $this->_getCategoryAttribute('is_active', array_keys($categories),
                    $category->getStoreId());
                unset($category);
                foreach ($attributes as $categoryId => $attributeValue) {
                    if($attributeValue == 0)
                    {
                        unset($categories[$categoryId]);
                    }
                }
               unset($attributes);
            }
            return $categories;
        }
        return parent::_getCategories($categoryIds, $storeId, $path);
    }
    
    /**
     * get all defined Product Data from array per storeview
     * 
     * @param array $ids
     * @param int $storeId
     * @return array
     */
    public function getProductsByIds($productIds, $storeId, &$lastEntityId)
    {
        return $this->_getProducts($productIds, $storeId, $lastEntityId, $lastEntityId);
    }
    
    /**
     * Retrieve Product data objects
     * LOE: remove if status(=2) is disabled or visibility(=1) false
     *
     * @param int|array $productIds
     * @param int $storeId
     * @param int $entityId
     * @param int $lastEntityId
     * @return array
     */
    protected function _getProducts($productIds, $storeId, $entityId, &$lastEntityId)
    {
        $products   = array();
        $websiteId  = Mage::app()->getStore($storeId)->getWebsiteId();
        $adapter    = $this->_getReadAdapter();


        if ($productIds !== null) {
            if (!is_array($productIds)) {
                $productIds = array($productIds);
            }
        }
        $bind = array(
            'website_id'        => (int)$websiteId,
            'entity_id'         => (int)$entityId
        );
        $select = $adapter->select()
            ->useStraightJoin(true)
            ->from(array('e' => $this->getTable('catalog/product')), array('entity_id'))
            ->join(
                array('w' => $this->getTable('catalog/product_website')),
                'e.entity_id = w.product_id AND w.website_id = :website_id',
                array()
            )
            ->where('e.entity_id > :entity_id')
            ->order('e.entity_id')
            ->limit($this->_productLimit);
        if ($productIds !== null) {
            $select->where('e.entity_id IN(?)', $productIds);
        }

        //if we are to ignore disabled products... add the necessary joins and conditions
        if($this->_helper()->HideDisabledProducts($storeId))
        {
            $statusCode = Mage::getResourceModel('eav/entity_attribute')->getIdByCode('catalog_product', 'status');
            $bind['status_id'] = (int)$statusCode;
            $bind['disabled' ] = Mage_Catalog_Model_Product_Status::STATUS_DISABLED;
            $bind['store_id'] = (int)$storeId;
            $bind['default_store_id'] = 0;
            $select->joinLeft(
                array('s' => $this->getTable(array('catalog/product', 'int'))),
                'e.entity_id = s.entity_id AND s.attribute_id = :status_id AND s.store_id = :store_id',
                array()
            );
            $select->joinLeft(
                array('ds' => $this->getTable(array('catalog/product', 'int'))),
                'e.entity_id = ds.entity_id AND ds.attribute_id = :status_id AND ds.store_id = :default_store_id',
                array()
            );
            $select->where('s.value <> :disabled OR (s.value IS NULL AND ds.value <> :disabled)');
        }

        //if we are to ignore not visible products... add the necessary joins and conditions
        if($this->_helper()->HideNotVisibileProducts($storeId))
        {
            $visibilityCode = Mage::getResourceModel('eav/entity_attribute')->getIdByCode('catalog_product', 'visibility');
            $bind['not_visible'] = Mage_Catalog_Model_Product_Visibility::VISIBILITY_NOT_VISIBLE;
            $bind['visibility_id'] = (int)$visibilityCode;
            $bind['store_id'] = (int)$storeId;
            $bind['default_store_id'] = 0;

            $select->joinLeft(
                array('v' => $this->getTable(array('catalog/product', 'int'))),
                'e.entity_id = v.entity_id AND v.attribute_id = :visibility_id AND v.store_id = :store_id',
                array()
            );
            $select->joinLeft(
                array('dv' => $this->getTable(array('catalog/product', 'int'))),
                'e.entity_id = dv.entity_id AND dv.attribute_id = :visibility_id AND dv.store_id = :default_store_id',
                array()
            );
            $select->where('v.value <> :not_visible OR (v.value IS NULL AND dv.value <> :not_visible)');
        }


        $rowSet = $adapter->fetchAll($select, $bind);
        foreach ($rowSet as $row) {
            $product = new Varien_Object($row);
            $product->setIdFieldName('entity_id');
            $product->setCategoryIds(array());
            $product->setStoreId($storeId);
            $products[$product->getId()] = $product;
            $lastEntityId = $product->getId();
        }

        unset($rowSet);

        if ($products) {
            $select = $adapter->select()
                ->from(
                    $this->getTable('catalog/category_product'),
                    array('product_id', 'category_id')
                )
                ->where('product_id IN(?)', array_keys($products));
            $categories = $adapter->fetchAll($select);
            foreach ($categories as $category) {
                $productId = $category['product_id'];
                $categoryIds = $products[$productId]->getCategoryIds();
                $categoryIds[] = $category['category_id'];
                $products[$productId]->setCategoryIds($categoryIds);
            }

            foreach (array('name', 'url_key', 'url_path') as $attributeCode) {
                $attributes = $this->_getProductAttribute($attributeCode, array_keys($products), $storeId);
                foreach ($attributes as $productId => $attributeValue) {
                    $products[$productId]->setData($attributeCode, $attributeValue);
                }
            }
        }

        return $products;
    }

    /**
     * Retrieve categories data objects by their ids. Return only categories that belong to specified store.
     * // LOE: Check Categories, force array output
     * @see Mage_Catalog_Model_Resource_Url::getCategories()
     *
     * @param int|array $categoryIds
     * @param int $storeId
     * @return array
     */
    public function getCategories($categoryIds, $storeId)
    {
        if($this->_helper()->DoNotUseCategoryPathInProduct($storeId))
        {
            return array();
        }
        $parent = parent::getCategories($categoryIds, $storeId);
        if(!$parent)
        {
            return array();
        }
        return $parent;
    }
    
    /**
     * Save rewrite URL
     *
     * @param array $rewriteData
     * @param int|Varien_Object $rewrite
     * @return Loewenstark_UrlIndexer_Model_Resource_Url
     */
    public function saveRewrite($rewriteData, $rewrite)
    {
        parent::saveRewrite($rewriteData, $rewrite);
        if($this->_helper()->OptimizeCategoriesLeftJoin($rewriteData['store_id']))
        {
            $this->_saveUrlIndexerRewrite($rewriteData, $rewrite);
        }
        return $this;
    }

    /**
     * Save urlindexer rewrite URL
     *
     * @param array $rewriteData
     * @param int|Varien_Object $rewrite
     * @return Loewenstark_UrlIndexer_Model_Resource_Url
     */
    protected function _saveUrlIndexerRewrite($rewriteData, $rewrite)
    {
        // check if is a category
        if((isset($rewriteData['category_id']) && !empty($rewriteData['category_id']))
         && isset($rewriteData['is_system']) && intval($rewriteData['is_system']) == 1
         && ((isset($rewriteData['product_id']) && is_null($rewriteData['product_id']))
             || !isset($rewriteData['product_id'])))
        {
            $adapter = $this->_getWriteAdapter();
            try {
                $adapter->insertOnDuplicate($this->getTable('urlindexer/url_rewrite'), $rewriteData);
            } catch (Exception $e) {
                Mage::logException($e);
                Mage::throwException(Mage::helper('urlindexer')->__('An error occurred while saving the URL rewrite in urlindexer'));
            }
            
            // delete old entry!
            if ($rewrite && $rewrite->getId()) {
                if ($rewriteData['request_path'] != $rewrite->getRequestPath()) {
                    // Update existing rewrites history and avoid chain redirects
                    $where = array('target_path = ?' => $rewrite->getRequestPath());
                    if ($rewrite->getStoreId()) {
                        $where['store_id = ?'] = (int)$rewrite->getStoreId();
                    }
                    $adapter->delete(
                        $this->getTable('urlindexer/url_rewrite'),
                        $where
                    );
                }
            }
        }
    }

    /**
     * 
     * @return Loewenstark_UrlIndexer_Helper_Data
     */
    protected function _helper()
    {
        return Mage::helper('urlindexer');
    }
}
