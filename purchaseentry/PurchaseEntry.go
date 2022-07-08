package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	purchaseentry "github.com/leapforce-libraries/go_exactonline_new/purchaseentry"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type PurchaseEntry struct {
	SoftwareClientLicenseGuid_  string
	Created_                    time.Time
	Modified_                   time.Time
	EntryID                     string
	AmountDC                    float64
	AmountFC                    float64
	BatchNumber                 int32
	Created                     bigquery.NullTimestamp
	Creator                     string
	CreatorFullName             string
	Currency                    string
	Description                 string
	Division                    int32
	Document                    string
	DocumentNumber              int32
	DocumentSubject             string
	DueDate                     bigquery.NullTimestamp
	EntryDate                   bigquery.NullTimestamp
	EntryNumber                 int32
	ExternalLinkDescription     string
	ExternalLinkReference       string
	GAccountAmountFC            float64
	InvoiceNumber               int32
	Journal                     string
	JournalDescription          string
	Modified                    bigquery.NullTimestamp
	Modifier                    string
	ModifierFullName            string
	OrderNumber                 int32
	PaymentCondition            string
	PaymentConditionDescription string
	PaymentReference            string
	ProcessNumber               int32
	Rate                        float64
	ReportingPeriod             int16
	ReportingYear               int16
	Reversal                    bool
	Status                      int16
	StatusDescription           string
	Supplier                    string
	SupplierName                string
	Type                        int32
	TypeDescription             string
	VATAmountDC                 float64
	VATAmountFC                 float64
	YourRef                     string
}

func getPurchaseEntry(c *purchaseentry.PurchaseEntry, softwareClientLicenseGuid string) PurchaseEntry {
	t := time.Now()

	return PurchaseEntry{
		softwareClientLicenseGuid,
		t, t,
		c.EntryID.String(),
		c.AmountDC,
		c.AmountFC,
		c.BatchNumber,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		c.Description,
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		go_bigquery.DateToNullTimestamp(c.DueDate),
		go_bigquery.DateToNullTimestamp(c.EntryDate),
		c.EntryNumber,
		c.ExternalLinkDescription,
		c.ExternalLinkReference,
		c.GAccountAmountFC,
		c.InvoiceNumber,
		c.Journal,
		c.JournalDescription,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.OrderNumber,
		c.PaymentCondition,
		c.PaymentConditionDescription,
		c.PaymentReference,
		c.ProcessNumber,
		c.Rate,
		c.ReportingPeriod,
		c.ReportingYear,
		c.Reversal,
		c.Status,
		c.StatusDescription,
		c.Supplier.String(),
		c.SupplierName,
		c.Type,
		c.TypeDescription,
		c.VATAmountDC,
		c.VATAmountFC,
		c.YourRef,
	}
}

func (service *Service) WritePurchaseEntries(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PurchaseEntryService().NewGetPurchaseEntriesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		purchaseEntries, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if purchaseEntries == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *purchaseEntries {
			batchRowCount++

			b, err := json.Marshal(getPurchaseEntry(&tl, softwareClientLicenseGuid))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#PurchaseEntries flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#PurchaseEntries: %v\n", rowCount)

	return objectHandles, rowCount, PurchaseEntry{}, nil
}
