package com.stream.common.domain;

import java.io.Serializable;

/**
 * @author zeyang_jia
 * @date 2025-05-04 14:00
 * @className: HBaseTable
 * @description HBaseTable
 */
public class HBaseTable implements Serializable {
    private String ethBlock;
    private String ethTranNormalByHash;
    private String ethTranInternalByHash;
    private String ethTranNormalByAddress;
    private String ethTranInternalByAddress;

    public HBaseTable() {
    }

    public HBaseTable(String ethBlock, String ethTranNormalByHash, String ethTranInternalByHash, String ethTranNormalByAddress, String ethTranInternalByAddress) {
        this.ethBlock = ethBlock;
        this.ethTranNormalByHash = ethTranNormalByHash;
        this.ethTranInternalByHash = ethTranInternalByHash;
        this.ethTranNormalByAddress = ethTranNormalByAddress;
        this.ethTranInternalByAddress = ethTranInternalByAddress;
    }

    public String getEthBlock() {
        return ethBlock;
    }

    public void setEthBlock(String ethBlock) {
        this.ethBlock = ethBlock;
    }

    public String getEthTranNormalByHash() {
        return ethTranNormalByHash;
    }

    public void setEthTranNormalByHash(String ethTranNormalByHash) {
        this.ethTranNormalByHash = ethTranNormalByHash;
    }

    public String getEthTranInternalByHash() {
        return ethTranInternalByHash;
    }

    public void setEthTranInternalByHash(String ethTranInternalByHash) {
        this.ethTranInternalByHash = ethTranInternalByHash;
    }

    public String getEthTranNormalByAddress() {
        return ethTranNormalByAddress;
    }

    public void setEthTranNormalByAddress(String ethTranNormalByAddress) {
        this.ethTranNormalByAddress = ethTranNormalByAddress;
    }

    public String getEthTranInternalByAddress() {
        return ethTranInternalByAddress;
    }

    public void setEthTranInternalByAddress(String ethTranInternalByAddress) {
        this.ethTranInternalByAddress = ethTranInternalByAddress;
    }

    @Override
    public String toString() {
        return "HBaseTable{" +
                "ethBlock='" + ethBlock + '\'' +
                ", ethTranNormalByHash='" + ethTranNormalByHash + '\'' +
                ", ethTranInternalByHash='" + ethTranInternalByHash + '\'' +
                ", ethTranNormalByAddress='" + ethTranNormalByAddress + '\'' +
                ", ethTranInternalByAddress='" + ethTranInternalByAddress + '\'' +
                '}';
    }
}
